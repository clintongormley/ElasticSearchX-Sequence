package ElasticSearchX::Sequence;

use strict;
use warnings;
use Carp;
use ElasticSearchX::Sequence::Iterator();

our $VERSION = 0.01;

#===================================
sub new {
#===================================
    my $class  = shift;
    my %params = (
        index => 'sequence',
        type  => 'sequence',
        ref $_[0] ? %{ shift() } : @_
    );
    my $self = bless {}, $class;
    for (qw(index type es)) {
        $self->{"_$_"} = $params{$_}
            or croak "Missing required param $_";
    }
    return $self;
}

#===================================
sub sequence {
#===================================
    my $self   = shift;
    my %params = (
        size => 100,
        (     ref $_[0] ? %{ shift() }
            : @_ == 1 ? ( name => shift )
            : @_
        ),
        sequence => $self,
    );
    return ElasticSearchX::Sequence::Iterator->new( \%params );
}

#===================================
sub bootstrap {
#===================================
    my $self = shift;
    my %params = ref $_[0] eq 'HASH' ? %{ shift() } : @_;
    %params = (
        auto_expand_replicas => '0-all',
        number_of_shards     => 1,
    ) unless %params;

    my $es      = $self->es;
    my $index   = $self->index;
    my $type    = $self->type;
    my $mapping = {
        _all    => { enabled => 0 },
        _source => { enabled => 0 },
        _type   => { index   => 'no' },
        enabled => 0,
    };
    if ( !$es->index_exists( index => $index ) ) {
        $es->create_index(
            index    => $index,
            settings => \%params,
            mappings => { $type => $mapping }
        );
    }
    else {
        $es->put_mapping(
            index   => $index,
            type    => $type,
            mapping => $mapping
        );
    }
    return $self;
}

#===================================
sub index { shift->{_index} }
sub type  { shift->{_type} }
sub es    { shift->{_es} }
#===================================

#===================================
sub delete_type {
#===================================
    my $self = shift;
    $self->es->delete_mapping(
        index          => $self->index,
        type           => $self->type,
        ignore_missing => 1
    );
    return $self;
}

#===================================
sub delete_index {
#===================================
    my $self = shift;
    $self->es->delete_index( index => $self->index, ignore_missing => 1 );
    return $self;
}

# ABSTRACT: Fast integer ID sequences with ElasticSearch

=head1 DESCRIPTION

L<ElasticSearchX::Sequence> gives you a sequence of auto-incrementing integers
(eg to use as IDs) that are guaranteed to be unique across your application.

It is similar in spirit to  L<DBIx::Sequence>, but uses ElasticSearch as a
backend.


=head1 SYNOPSIS

    use ElasticSearch();
    use ElasticSearchX::Sequence();

    my $es  = ElasticSearch->new();
    my $seq = ElasticSearchX::Sequence->new( es => $es );

    $seq->bootstrap();

    my $it  = $seq->sequence('mail_id');

    my $mail_id = $it->next;

=head1 MOTIVATION

ElasticSearch already has built in unique IDs, but they look like this:
C<KpSb_Jd_R56dH5Qx6TtxVA>.

If you are migrating from an RDBM where you are using (eg) an auto-increment
column to give you unique IDs, your application may depend on these IDs being
integers. Or you may just prefer integer IDs.

Either way, this module makes it easy to get these unique auto-incrementing
IDs without needing an RDBM to provide them.

And it is fast! Given the performance, if you are already using ElasticSearch,
you may want to move your ticket servers from your database to ElasticSearch
instead.

=head1 PERFORMANCE

This module is blazing fast, especially when L<ElasticSearch> uses the
L<ElasticSearch::Transport::Curl> backend.

You can try out the benchmark yourself, in the C<benchmark> folder in this
distribution.

The script compares:

=over

=item *

MySQL, using the L<ticket method described by Flickr|http://code.flickr.com/blog/2010/02/08/ticket-servers-distributed-unique-primary-keys-on-the-cheap/>

=item *

this module, using the L<httptiny|ElasticSearch::Transport::HTTPTiny> backend

=item *

this module, using the L<curl|ElasticSearch::Transport::Curl> backend

=item *

this module, using the L<curl|ElasticSearch::Transport::Curl> backend but
only requesting blocks of 10 IDs at a time

=back

The results I get when running this on my laptop are:

                   Rate es_curl_10  db_ticket    es_tiny    es_curl
    es_curl_10  38760/s         --       -48%       -55%       -72%
    db_ticket   74627/s        93%         --       -13%       -47%
    es_tiny     85470/s       121%        15%         --       -39%
    es_curl    140845/s       263%        89%        65%         --

Plus, with ElasticSearch, you get distributed and high-availability thrown in
for free.

=head1 METHODS

=head2 new()

    my $seq = ElasticSearchX::Sequence->new(
        es      => $es,         # ElasticSearch instance, required
        index   => 'index',     # defaults to 'sequence',
        type    => 'type',      # defaults to 'sequence',
    );

C<new()> returns a new instance of L<ElasticSearchX::Sequence>. By default,
your sequences will be stored in index C<sequence>, type C<sequence>, but
you can change those values to whatever suits your application.

By default, the index is optimised for serving sequences, and has different
settings than those you would typically use in your main index, so rather than
storing your sequences in the main index for your application(s), you may prefer
to store all of your sequences in the single index C<sequence>.

The type (default C<sequence>) could be used to separate sequences for
different applications. For instance, you could store the sequences for
your personal blog in type C<personal> and for your work blog in type C<work>.

See L</"bootstrap()"> for how to initiate your index/type.

=head2 sequence()

    my $it = $seq->sequence('mail_id');
    my $it = $seq->sequence( name => 'mail_id', size => 100 );

The C<sequence()> method returns a new sequence iterator identified by the
C<name>.

New IDs/values are generated in blocks of C<size> (default 100), as this is
much faster than requesting them individually.

This does mean that, if you have several instances of the iterator C<mail_id>,
then the next ID won't always be the highest number available.  For instance:

    $i_1 = $seq->('mail_id');
    $i_2 = $seq->('mail_id');

    say $i_1->next;
    say $i_2->next;
    say $i_1->next;
    # 1
    # 101
    # 2

See also L<ElasticSearchX::Sequence::Iterator>.

=head2 bootstrap()

    $seq->bootstrap( %settings );

This method will create the index, if it doesn't already exist, and will
setup the type.  This can be called even if the index and type have already
been setup. It won't fail unless the type already exists and has a different
mapping / definition.

By default, the index is setup with the following C<%settings>:

    (
        number_of_shards     => 1,
        auto_expand_replicas => '0-all',
    )

In other words, it will have only a single primary shard (instead of the
ElasticSearch default of 5), and a replica of that shard on every ElasticSearch
node in your cluster.

If you pass in any C<%settings> then the defaults will not be used at all.

See L<Index Settings|http://www.elasticsearch.org/guide/reference/api/admin-indices-update-settings.html> for more.

=head2 delete_index()

    $seq->delete_index()

Deletes the index associated with the sequence. You will lose your data!

=head2 delete_type()

    $seq->delete_type()

Deletes the type associated with the sequence. You will lose your data!

=head2 index()

    $index = $seq->index

Read-only getter for the index value

=head2 type()

    $type = $seq->type

Read-only getter for the type value

=head2 es()

    $es = $seq->es

Read-only getter for the ElasticSearch instance.

=head1 SEE ALSO

L<ElasticSearch>, L<http://www.elasticsearch.org>

=head1 BUGS

If you have any suggestions for improvements, or find any bugs, please report
them to L<https://github.com/clintongormley/ElasticSearchX-Sequence/issues>.
I will be notified, and then you'll automatically be notified of progress on
your bug as I make changes.

=cut

1;