use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
              'NAME'		=> 'DB2::db',
              'VERSION_FROM'	=> 'db.pm', # finds $VERSION
              'PREREQ_PM'       => {
                  'DBD::DB2' => 0.76,
              },
              'AUTHOR'          => 'Darin McBride (darin@naboo.to.org)',
              'ABSTRACT'        => 'OO Framework for building DBI applications using IBM DB2',
              clean => {
                  # I use fte (http://sf.net/fte) which leaves ~ backup files around.
                  FILES => '*~ t/*~ samples/*~ samples/My/*~',
              },
             );

