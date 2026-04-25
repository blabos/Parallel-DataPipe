use strict;
use warnings;

# Regression tests for two framing-correctness bugs in _get_data / _put_data.
#
# 1. test_partial_header_syswrite
#    _put_data does not check the return value of syswrite() when writing
#    the 4-byte frame header or EOF marker. A POSIX-legal partial write
#    silently corrupts framing: the reader assembles some header bytes with
#    some payload bytes, mis-sizes the message, and blocks forever.
#    _put_data must die if the header syswrite does not transfer exactly 4
#    bytes.
#
# 2. test_zero_byte_payload_sysread
#    When sysread() returns 0 (pipe closed, genuine EOF) inside the payload
#    accumulation loop of _get_data, $offset does not advance, the loop
#    condition stays true, and _get_data hangs forever. _get_data must die
#    on zero-byte progress rather than loop indefinitely.
#
# Both tests call _get_data / _put_data directly on a minimal DataPipe
# object (no workers forked) to isolate each failure mode precisely.

use IO::Handle;
use Test::More tests => 4;
use Parallel::DataPipe;

use constant TIMEOUT => 10;

test_partial_header_syswrite();
test_zero_byte_payload_sysread();

exit 0;

# Returns a bare DataPipe object with serialiser initialised but no workers.
sub _make_dp {
    my $dp = bless {}, 'Parallel::DataPipe';
    $dp->init_serializer( {} );
    return $dp;
}

# _put_data must die when syswrite() transfers fewer than 4 bytes for the
# 4-byte frame header.  Before the fix the short write is silently ignored.
sub test_partial_header_syswrite {
    note 'Testing _put_data dies on partial syswrite of the 4-byte frame header.';

    my ( $r, $w );
    pipe( $r, $w ) or die "pipe: $!";

    my $dp = _make_dp();

    my ( $partial_wrote, $ok );
    {
        no warnings 'redefine';

        # When exactly 4 bytes are requested (the frame header or EOF marker),
        # write only 2 and return 2 -- a POSIX-legal partial syswrite.
        local *IO::Handle::syswrite = sub {
            my $requested = defined $_[2] ? $_[2] : length( $_[1] );
            if ( $requested == 4 ) {
                $partial_wrote = CORE::syswrite( $_[0], substr( $_[1], 0, 2 ), 2 );
                return $partial_wrote;
            }
            return defined $_[2]
              ? CORE::syswrite( $_[0], $_[1], $_[2] )
              : CORE::syswrite( $_[0], $_[1], length( $_[1] ) );
        };

        $ok = eval { $dp->_put_data( $w, 'regression-payload' ); 1 };
    }

    ok( !$ok,
        '_put_data dies when frame header syswrite returns fewer than 4 bytes' )
      or diag('_put_data returned without error; partial header write was silently ignored');

    ok( defined($partial_wrote) && $partial_wrote == 2,
        'injection confirmed: syswrite returned 2 for the 4-byte header' );

    close $r;
    close $w;
}

# _get_data must die rather than loop forever when sysread() returns 0
# (unexpected EOF) inside the payload accumulation loop.
sub test_zero_byte_payload_sysread {
    note 'Testing _get_data dies on zero-byte sysread during payload accumulation.';

    my ( $r, $w );
    pipe( $r, $w ) or die "pipe: $!";

    my $dp = _make_dp();

    # Write a valid header declaring a 500-byte payload, then close the write
    # end.  sysread in the payload loop returns 0 immediately (EOF).
    $w->syswrite( pack( 'l', 500 ) );
    close($w);

    my $timed_out = 0;
    local $SIG{ALRM} = sub { $timed_out = 1; die "TIMEOUT after " .TIMEOUT . "s\n" };
    alarm(TIMEOUT);

    my $ok = eval { $dp->_get_data($r); 1 };
    alarm(0);

    close($r);

    ok( !$timed_out,
        '_get_data did not loop forever on zero-byte sysread in payload' )
      or diag("_get_data blocked indefinitely; killed by SIGALRM after " .TIMEOUT . "s");

    ok( !$ok, '_get_data died with an error on unexpected EOF during payload read' )
      or diag('_get_data returned without error despite incomplete payload');
}
