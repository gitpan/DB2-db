#! /usr/bin/perl

use Module::Build;
my $build = Module::Build->new
    (
     module_name => 'DB2::db',
     license => 'perl',
     requires => {
         DBD::DB2 => '0.78',
     },
    );

$build->create_build_script();

