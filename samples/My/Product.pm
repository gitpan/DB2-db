package My::Product;

use base 'My::Table';

sub data_order
{
    [
     {
         COLUMN => 'PRODNAME',
         TYPE   => 'VARCHAR',
         LENGTH => '30',
         OPTS   => 'NOT NULL',
     },
     {
         COLUMN => 'BASEPRICE',
         TYPE   => 'DECIMAL',
         LENGTH => '8,2',
     },
     { 
         COLUMN => 'PRODID',
         TYPE   => 'INTEGER',
         GENERATEDIDENTITY => undef,
     },
    ];
};

sub get_base_row_type { 'My::Row' };

1;
