package My::Employee;

use My::Table;

our @ISA = qw(My::Table);

=head1 USAGE

Please note that the table name, unless overridden, is the same as the
package name after the final ::'s.  For example, the table name for this
table is "Employee".  (Actually, table names aren't usually case sensitive,
so this automatically changes to "EMPLOYEE".)

=cut

sub data_order
{
    [
     {
         COLUMN => 'EMPNO',
         TYPE   => 'CHAR',
         LENGTH => 6,
         OPTS   => 'NOT NULL',
         PRIMARY => 1,
     },
     {
         COLUMN => 'FIRSTNAME',
         TYPE   => 'CHAR',
         LENGTH => 12,
         OPTS   => 'NOT NULL',
     },
     {
         COLUMN => 'MIDINIT',
         TYPE   => 'CHAR',
     },
     {
         COLUMN => 'LASTNAME',
         TYPE   => 'CHAR',
         LENGTH => 15,
         OPTS   => 'NOT NULL',
     },
     {
         COLUMN => 'SALARY',
         TYPE   => 'DECIMAL',
         LENGTH => '8,2',
     },
    ];
}

1;
