#! perl -w

use Test::More tests => 5;

BEGIN
{
    use_ok('DBD::DB2');

    use_ok('DB2::db');
    use_ok('DB2::Table');
    use_ok('DB2::Row');
}

can_ok('DB2::db', qw(new));


