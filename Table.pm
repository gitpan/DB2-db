package DB2::Table;

use diagnostics;
use strict;
use Carp;

our $VERSION = '0.17';

=head1 NAME

DB2::Table - Framework wrapper around tables using DBD::DB2

=head1 SYNOPSIS

    package myTable;
    use DB2::Table;
    our @ISA = qw( DB2::Table );
    
    ...
    
    use myDB;
    use myTable;
    
    my $db = myDB->new;
    my $tbl = $db->get_table('myTable');
    my $row = $tbl->find($id);

=head1 FUNCTIONS

=over 4

=item C<new>

Do not call this - you should get your table through your database object.

=cut

sub new
{
    my $class = shift;
    my $self = {};
    bless $self, ref $class || $class || confess("Unknown table");

    my $db = shift;
    confess("Need the db handle as parameter")
        unless $db and ref $db and $db->isa("DB2::db");
    $self->{db} = $db;

    my %tableOrder;
    my @cl = $self->column_list;
    @tableOrder{ @cl } = (0..$#cl);
    $self->{tableOrder} = \%tableOrder;

    $self;
}

=item C<data_order>

the key sub to override!  The data must be a reference to an array of hashes.  Each
element (hash) in the array must contain certain keys, others are optional.

=over 2

=item Required:

=over 2

=item C<COLUMN>

Column Name (must be upper case)

=item C<TYPE>

SQL type

=back

=item Optional:

=over 2

=item C<LENGTH>

for CHAR, VARCHAR, etc.

=item C<OPTS>

optional stuff - C<NOT NULL>, C<PRIMARY KEY>, etc.

=item C<DEFAULT>

default value

=item C<PRIMARY>

true for the primary key

=item C<CONSTRAINT>

stuff that is placed in the table create independantly

=item C<FOREIGNKEY>

For this column, will create a FOREIGN KEY statement.  The value here
is used during creation of the table, and should begin with the foreign
table name and include any "ON DELETE", "ON UPDATE", etc., portions. 
This may change in the future where FOREIGNKEY will be itself another
hashref with all these fields.

=item C<GENERATEDIDENTITY>

For this column, will create as a generated identity.  If this is undef
or the word 'default', the option will be C<(START WITH 0, INCREMENT BY 1, NO CACHE)>,
otherwise it will use whatever you provide here.

=back

=back

This is somewhat based on a single column for a primary key, which is not
necessarily the "right" thing to do in relational design, but sure as heck
simplifies coding!
NOTE: Other columns may be present, but would only be used by the subclass.

=cut

sub data_order
{
    die "Gotta override data_order!";
}

sub _internal_data_order
{
    my $self = shift;
    unless ($self->{_data_order})
    {
        $self->{_data_order} = $self->data_order();
    }
    $self->{_data_order};
}

sub _internal_data_reset
{
    my $self = shift;
    delete $self->{_data_order};
    delete $self->{column_list};
    delete $self->{ALL_DATA};
    delete $self->{PRIMARY};
    delete $self->{GENERATEDIDENTITY};
}

=item C<get_base_row_type>

When allowing the framework to create your row type object because there
is no backing module, we need to know what to derive it from.  If you have
a generic row type that is derived from DB2::Row that you want all your
rows to be derived from, you can override this.

If all your empty Row types are derived from a single type that is not
DB2::Row, you should create a single Table type and have all your tables
derived from that.  That is, to create a derivation tree for your row such as:

    DB2::Row -> My::Row -> My::UserR

your derivation tree for your tables should look like:

    DB2::Table -> My::Table -> My::User

And then C<My::Table> can override C<get_base_row_type> to return
C<q(My::Row)>

=cut

sub get_base_row_type
{
    q(DB2::Row);
}


=item C<getDB>

Gets the DB2::db object that contains this table

=cut

sub getDB
{
    shift->{db};
}

=item C<schema_name>

You need to override this.  Must return the DB2 Schema to use for this
table.  Generally, you may want to derive a single "schema" class from
DB2::Table which only overrides this method, and then derive each table
in that schema from that class.

=cut

sub schema_name { confess("You must override schema_name") }

sub _connection
{
    my $self = shift;
    $self->getDB->connection;
}

sub _find_create_row
{
    my $self = shift;
    my $type = ref $self;
    $type = $self->{db}->get_row_type_for_table($type);

    my @row = @_;

    my %params = ( _db_object => $self->getDB );
    my $data_order = $self->_internal_data_order();
    foreach my $i (0..$#$data_order)
    {
        my $column = $data_order->[$i]{COLUMN};
        if (defined $row[$i])
        {
            ($params{$column} = $row[$i]) =~ s/\s*$//;
        }
    }

    return $type->new(\%params);
}

=item C<create_row>

Creates a new DB2::Row object for this table.  Called instead of the
constructor for the DB2::Row object.  Sets up defaults, etc.  B<NOTE>:
this will not generate any identity column!  We leave that up to the
database, so we will retrieve that during the save before committing.

=cut

sub create_row
{
    my $self = shift;

    $self->_find_create_row( map 
                            {
                                $self->get_column($_, 'DEFAULT');
                            } $self->column_list );
}

=item C<count>

Should be obvious - a full count of all the rows in this table

=cut

sub count
{
    my $self = shift;

    $self->SELECT('COUNT(*)')->[0][0];
}

=item C<count_where>

Similar to C<count>, except that the first parameter will be the SQL
WHERE condition while the rest of the parameters will be the bind
values for that WHERE condition.

=cut

sub count_where
{
    my $self = shift;

    $self->SELECT('COUNT(*)', @_)->[0][0];
}

=item C<find_id>

Finds all rows with the primary column matching any of the parameters. 
For example, $tbl->find_id(1, 2, 10) will return an array of DB2::Row
derived objects with all the data from 0-3 rows from this table, if
the primary column for that row is either 1, 2, or 10.

=cut

sub find_id
{
    my $self = shift;

    $self->find_where(
                      $self->primaryColumn . ' IN (' .
                      join (', ', map {'?'} @_) . ')',
                      @_
                     );
}

=item C<find_where>

Similar to C<find_id>, the first parameter is the SQL WHERE condition
while the rest of the parameters are the bind values for the WHERE
condition.

In array context, will return the array of DB2::Row derived objects
returned, whether empty or not.

In scalar context, will return undef if no rows are found, will return
the single Row object if only one row is found, or an array ref if more
than one row is found.

=cut

sub find_where
{
    my $self = shift;
    $self->find_join($self->full_table_name, @_);
}

=item C<find_join>

Similar to C<find_where>, the first parameter is the tables to join
and how they are joined (any '!!!' found will be replaced with the
current table's full name), the second parameter is the where condition,
if any, and the rest are bind values.

=cut

sub find_join
{
    my $self = shift;

    my @cols = $self->column_list();
    my $prefix = "";
    if ($_[0] and (
                   $_[0] =~ /!!!\s+[Aa][Ss]\s+(\w+)/ or
                   $_[0] =~ /$self->full_table_name()\s+[Aa][Ss]\s+(\w+)/ or
                   $_[0] =~ /$self->table_name()\s+[Aa][Ss]\s+(\w+)/
                  )
        )
    {
        $prefix = "$1.";
    }

    my $ary_ref = $self->SELECT_join("distinct " . join(', ', map {$prefix . $_} $self->column_list),
                                     @_);

    my @rc;
    foreach my $row (@$ary_ref)
    {
        push @rc, $self->_find_create_row(@$row);
    }

    # array, empty or not.
    if (wantarray)
    {
        return @rc;
    }
    # if there aren't any, send back undef.
    if (scalar @rc < 1)
    {
        return undef;
    }
    # no array wanted, and only one answer, send it back.
    if (scalar @rc == 1)
    {
        return $rc[0];
    }
    # no array wanted, send back ref to array.
    return \@rc;
}

=item C<_prepare>

Internally used to cache statements (future).  This may change to
C<prepare> if it is found to be useful.

=cut

sub _prepare
{
    my $self = shift;
    my $stmt = shift;

    print "$stmt\n" if $DB2::db::debug;
    my $sth = $self->_connection->prepare($stmt);

    croak "Can't prepare [$stmt]: " . $self->_connection->errstr() unless $sth;

    $sth;
}

sub _execute
{
    my $self = shift;
    my $sth  = shift;

    delete $self->{_dbi};
    unless ($sth->execute(@_))
    {
        $self->{_dbi}{err} = $sth->err;
        $self->{_dbi}{errstr} = $sth->errstr;
        $self->{_dbi}{state} = $sth->state;
        undef;
    }
}

sub dbi_err    { shift->{_dbi}{err} }
sub dbi_errstr { shift->{_dbi}{errstr} }
sub dbi_state  { shift->{_dbi}{state} }

sub _already_exists_in_db
{
    my $self = shift;
    my $obj  = shift;

    my $dbh = $self->_connection;
    my $column = $self->primaryColumn;
    my $count = 0;

    if ($column)
    {
        my $objval = $obj->column($column);

        #my $stmt = "SELECT COUNT(*) FROM " . $self->full_table_name .
        #    " WHERE $column IN ?";
        #$count = $dbh->selectrow_array($stmt, undef, $objval);
        $count = $self->SELECT('COUNT(*)', "$column IN ?", $objval)->[0][0];
    }

    return $count;
}

sub _update_db
{
    my $self = shift;
    my $obj  = shift;

    # it's an update.
    my $stmt = "UPDATE " . $self->full_table_name . " SET ";
    my $prim_key = $self->primaryColumn;

    # find all modified fields.
    my @sets;
    my @newVal;

    for my $key (keys %{$obj->{modified}})
    {
        next if $key eq $prim_key;
        push @sets, "$key = ?";
        push @newVal, $obj->{CONFIG}{$key};
    }

    if (@sets)
    {
        $stmt .= join(", ", @sets);
        $stmt .= " WHERE " . $self->primaryColumn . " IN ?";
        push @newVal, $obj->{CONFIG}{$prim_key};
        my $sth = $self->_prepare($stmt);

        #print STDERR "stmt = $stmt -- ", join @newVal, "\n";

        $self->_execute($sth, @newVal);
    }
    else
    {
        '0E0'; # default return value.
    }
}

sub _insert_into_db
{
    my $self = shift;
    my $obj  = shift;

    my @cols = grep {
        not $self->get_column($_, 'NOCREATE') and
            $_ ne $self->generatedIdentityColumn()
    } $self->column_list;

    my $stmt = "INSERT INTO " . $self->full_table_name . " (" .
        join(', ', @cols) .
        ") VALUES(" . join(', ', map {'?'} @cols) . ")";

    print STDERR "$stmt\n" if $DB2::db::debug;

    my $sth = $self->_prepare($stmt);
    $self->_execute($sth, map { $obj->{CONFIG}{$_} } @cols);
}

=item C<save>

The table is what saves a row.  If you've made changes to a row, this
function will save it.  Not really needed since the Row's destructor
will save, but doesn't hurt.

=cut

sub save
{
    my $self = shift;
    my $obj  = shift;

    unless (ref $obj and $obj->isa("DB2::Row"))
    {
        croak("Got a " . ref($obj) . " which isn't a 'DB2::Row'");
    }

    if ($self->_already_exists_in_db($obj))
    {
        if ($self->primaryColumn)
        {
            $self->_update_db($obj);
        }
    }
    # else it's new
    else
    {
        $self->_insert_into_db($obj);
    }
}

=item C<commit>

Commits all current actions

=cut

sub commit
{
    my $self = shift;
    $self->_connection->commit;
}

=item C<delete>

Deletes the given row from the database.

=cut

sub delete
{
    my $self = shift;
    my $obj  = shift;

    unless (ref $obj and $obj->isa("DB2::Row"))
    {
        croak("Got a " . ref($obj) . " which isn't a 'DB2::Row'");
    }

    if ($self->_already_exists_in_db($obj))
    {
        $self->_delete_db($obj);
    }
}

sub _delete_db
{
    my $self = shift;
    my $obj  = shift;

    my $primcol = $obj->primaryColumn;
    if ($primcol)
    {
        my $stmt = 'DELETE FROM ' . $self->full_table_name . ' WHERE ' .
            $primcol . ' IN ?';

        my $sth = $self->_prepare($stmt);
        $self->_execute($sth, $obj->column($primcol));
    }
}

=item C<SELECT>

Wrapper around performing an SQL SELECT statement.  The first argument
is the columns you want, the next is the WHERE condition (undef if
none), and the rest are the bind values.  Will always return an array
ref.

=cut

sub SELECT
{
    my $self = shift;
    my $cols = shift;
    my $where = shift;
    my @params = @_;

    my $stmt = 'SELECT ' . $cols . ' FROM ' . $self->full_table_name;
    $stmt .= ' WHERE ' . $self->_replace_bangs($where) if $where;

    my $sth = $self->_prepare($stmt);
    $self->_execute($sth, @params);
    return $sth->fetchall_arrayref();
}

=item C<SELECT_distinct>

Wrapper around performing an SQL SELECT statement with distinct rows only
returned.  Otherwise, it's exactly the same as C<SELECT> above

=cut

sub SELECT_distinct
{
    my $self = shift;
    my $cols = shift;
    my $where = shift;
    my @params = @_;

    my $stmt = 'SELECT DISTINCT ' . $cols . ' FROM ' . $self->full_table_name;
    $stmt .= ' WHERE ' . $self->_replace_bangs($where) if $where;

    my $sth = $self->_prepare($stmt);
    $self->_execute($sth, @params);
    return $sth->fetchall_arrayref();
}

=item C<SELECT_join>

Wrapper around performing an SQL SELECT statement where you may be joining
with other tables.  The first argument is the columns you want, the second
is the tables, and how they are to be joined, while the third is the WHERE
condition.  Further parameters are bind values.  Any text matching '!!!' in
the columns text will be replaced with this table's full table name.  Any
text matching '!(\S+?)!' will be replaced with $1's full table name.

=cut

sub _replace_bangs
{
    my $self = shift;

    $_[0] =~ s/!!!/$self->full_table_name()/ge;
    $_[0] =~ s/!(\S+?)!/$self->getDB()->get_table("$1")->full_table_name()/ge;
    $_[0];
}

sub SELECT_join
{
    my $self   = shift;
    my $cols   = shift;
    my $tables = shift;
    my $where  = shift;

    $self->_replace_bangs($tables);

    my $stmt = 'SELECT ' . $cols . ' FROM ' . $tables;
    $stmt .= ' WHERE ' . $self->_replace_bangs($where) if $where;

    my $sth = $self->_prepare($stmt);
    $self->_execute($sth, @_);
    return $sth->fetchall_arrayref();
}

=item C<table_name>

The name of this table, excluding schema.  This will default to the
part of the current package after the last double-colon.  For example,
if your table is in package "myDB2::foo", then the table name will be
"foo".

=cut

sub table_name
{
    my $self = shift;
    unless (exists $self->{table_name})
    {
        my $type = ref $self;
        ( my $tbl = $type ) =~ s/.*::(\w+)/$1/;
        $self->{table_name} = uc $tbl;
    }
    $self->{table_name};
}

=item C<full_table_name>

Shortcut to schema.table_name

=cut

sub full_table_name
{
    my $self = shift; 
    unless (exists $self->{full_table_name})
    {
        $self->{full_table_name} = uc $self->schema_name . '.' . $self->table_name;
    }
    $self->{full_table_name};
}

=item C<column_list>

Returns an array of all the column names, in order

=cut

sub column_list
{
    my $self = shift;
    if (not exists $self->{column_list})
    {
        $self->{column_list} = [map { $_->{COLUMN} } @{$self->_internal_data_order}];
    }
    @{$self->{column_list}}
}

=item C<all_data>

Returns a hash ref which is all the data from C<data_order>, but in no
particular order (it's a hash, right?).

=cut

sub all_data
{
    my $self = shift;
    unless ($self->{ALL_DATA})
    {
        foreach my $h (@{$self->_internal_data_order()})
        {
            $self->{ALL_DATA}{$h->{COLUMN}} = $h;
        }
    }
    $self->{ALL_DATA}
}

=item C<get_column>

Gets information about a column or its data.  First parameter is the
column.  Second parameter is the key (NAME, TYPE, etc.).  If
the key is not given, a hash ref is returned with all the data for
this column.  If the key is given, only that scalar is returned.

=cut

sub get_column
{
    my $self = shift;
    my $column = uc shift;
    my $data = uc shift;
    my $all_data = $self->all_data;

    return undef unless exists $all_data->{$column};

    if ($data)
    {
        exists $all_data->{$column}{$data} ? $all_data->{$column}{$data} : undef;
    }
    else
    {
        $all_data->{$column};
    }
}

=item C<primaryColumn>

Find the primary column.  First time it is called, it will determine
the primary column, and then it will cache this for later calls.  If
you want a table with no primary column, you must override this method
to return undef.

If no column has the PRIMARY attribute, then the last column is
defaulted to be the primary column.

=cut

# Find the primary column (and cache it)
sub primaryColumn
{
    my $self = shift;
    # Check cache.
    if (not exists $self->{PRIMARY})
    {
        # default to last one.
        $self->{PRIMARY} = $self->_internal_data_order()->[$#{$self->_internal_data_order()}]{COLUMN};

        my $data_order = $self->_internal_data_order();
        for (my $i = 0; $i < scalar @$data_order; ++$i)
        {
            if (exists $data_order->[$i]{PRIMARY} and $data_order->[$i]{PRIMARY})
            {
                $self->{PRIMARY} = $data_order->[$i]{COLUMN};
                last;
            }
        }
    }
    $self->{PRIMARY};
}

=item C<generatedIdentityColumn>

Determine the generated identity column, if any.  This is determined by
looking for the string 'GENERATED ALWAYS AS IDENTITY' in the OPTS of
the column.  Again, this is cached on first use.

=cut

sub generatedIdentityColumn
{
    my $self = shift;
    if (not exists $self->{GENERATEDIDENTITY})
    {
        $self->{GENERATEDIDENTITY} = '';
        foreach my $col (@{$self->_internal_data_order()})
        {
            if (exists $col->{GENERATEDIDENTITY} or
                (
                 exists $col->{OPTS} and
                 $col->{OPTS} =~ /GENERATED ALWAYS AS IDENTITY/i)
               )
            {
                $self->{GENERATEDIDENTITY} = $col->{COLUMN};
                last;
            }
        }
    }
    $self->{GENERATEDIDENTITY};
}

# Get the hash describing a column

sub table_exists
{
    my $self = shift;
    my $dbh = $self->_connection;
    my @tables = $dbh->tables(
                              {
                                  TABLE_SCHEM => uc $self->schema_name,
                                  TABLE_NAME  => uc $self->table_name,
                              }
                             );
    die "Unexpected - more than one table with same schema/name!" if scalar @tables > 1;
    scalar @tables;
}

# INTERNAL - get current table structure (column names)
sub create_table_get_current
{
    my $self = shift;
    my $dbh = $self->_connection;

    my @row;
    if ($self->table_exists())
    {
        my $query = 'SELECT * FROM ' . $self->full_table_name . ' WHERE 1 = 0';
        my $sth  = $dbh->prepare($query);

        $self->_execute($sth);
        @row = @{$sth->{NAME}};
        $sth->finish;
    }
    @row;
}
# INTERNAL - common code between CREATE and ALTER - column definitions
sub create_table_column_definition
{
    my $self = shift;
    my $column = shift;
    my $tbl = $column->{COLUMN} . ' ';
    $tbl   .= uc $column->{TYPE} eq 'BOOL' ? 'CHAR' : $column->{TYPE};
    $tbl   .= ' (' . $column->{LENGTH} . ')' if exists $column->{LENGTH};
    $tbl   .= ' ' . $column->{OPTS} if $column->{OPTS};
    $tbl   .= ' CHECK (' . $column->{COLUMN} . q[ in ('Y','N'))] if uc $column->{TYPE} eq 'BOOL';
    if (exists $column->{GENERATEDIDENTITY})
    {
        $tbl .= ' GENERATED ALWAYS AS IDENTITY ';
        if (not defined $column->{GENERATEDIDENTITY} or 
            $column->{GENERATEDIDENTITY} eq 'default')
        {
            $tbl .= '(START WITH 0, INCREMENT BY 1, NO CACHE)';
        }
        else
        {
            $tbl .= $column->{GENERATEDIDENTITY};
        }
    }
    $self->_replace_bangs($tbl);
}
# Create the table as given by data_order.
sub create_table
{
    my $self = shift;
    my $dbh = $self->_connection;
    my %current_col_names = map { $_ => 1 } $self->create_table_get_current();

    if (scalar keys %current_col_names == 0)
    { # new table
        my $tbl = 'CREATE TABLE ' . $self->full_table_name . ' (';
        my @columns;
        my @constraints;
        my @foreign_keys;
        foreach my $f ( $self->column_list )
        {
            my $column = $self->get_column($f);
            push @columns, $self->create_table_column_definition($column);
            if (exists $column->{CONSTRAINT})
            {
                push @constraints, map { 
                    my $x = 'CONSTRAINT ' . $_;
                    $self->_replace_bangs($x);
                } ref($column->{CONSTRAINT}) eq 'ARRAY' ? @{$column->{CONSTRAINT}} : $column->{CONSTRAINT};
            }
            if (exists $column->{FOREIGNKEY})
            {
                push @foreign_keys, map {
                    my $x = 'FOREIGN KEY (' . $column->{COLUMN} . ') REFERENCES ' . $_;
                    $self->_replace_bangs($x);
                } ref($column->{FOREIGNKEY}) eq 'ARRAY' ? @{$column->{FOREIGNKEY}} : $column->{FOREIGNKEY};
            }
        }
        if ($self->primaryColumn)
        {
            push @constraints, 'PRIMARY KEY (' . $self->primaryColumn . ')';
        }
        $tbl .= join(', ', @columns, @constraints, @foreign_keys);
        $tbl .= ') DATA CAPTURE NONE';

        print "$tbl\n";
        unless ($dbh->do($tbl))
        {
            print $DBI::err, '[', $DBI::state, '] : ', $DBI::errstr, "\n";
        }

        $self->create_table_initialise('CREATE', $self->column_list());
    }
    else
    { # existing table - anything need to be updated?
        my $alter = 'ALTER TABLE ' . $self->full_table_name;
        my @add = grep { not exists $current_col_names{$_} } ($self->column_list);

        if (scalar @add)
        {
            foreach my $add (@add)
            {
                my $column = $self->get_column($add);
                $alter .= ' ADD ' . $self->create_table_column_definition($column);
            }
            print $alter, "\n";
            $dbh->do($alter);

            $self->create_table_initialise('ALTER', @add);
        }
    }

}

=item C<create_table_initialise>

A hook that will allow you to initialise the table immediately after
its creation.  If the table is newly created, the only parameter will
be 'CREATE'.  If the table is being altered, the first parameter will
be 'ALTER' while the rest of the parameters will be the list of columns
added.

The default action is mildly dangerous.  It grants full select, insert,
update, and delete authority to the user 'nobody'.  This is the user
that many daemons, including the default Apache http daemon, run under.
 If you override this, you can do whatever you want, including nothing.
 This default was put in primarily because many perl DBI scripts are
expected to also be CGI scripts, so this may make certain things
easier.  This does not change the fact that when this grant is executed
you will need some admin authority on the database.

=cut

sub create_table_initialise
{
    my $self = shift;
    my $action = shift;
    if ($action eq 'CREATE')
    {
        # default: grant authority to nobody (useful for web apps)
        my $grant =
            'GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE ' .
            $self->full_table_name .
            ' TO USER NOBODY';
        $self->_connection->do($grant);
    }

}

=back

=cut

1;
