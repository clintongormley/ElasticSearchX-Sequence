package ElasticSearchX::Sequence::Iterator;

use strict;
use warnings;
use Carp;

#===================================
sub new {
#===================================
    my $class    = shift;
    my $params   = shift;
    my $name     = $params->{name} or croak "No sequence name specified";
    my $size     = $params->{size} || 100;
    my $sequence = $params->{sequence};
    my $index    = $sequence->index;
    my $type     = $sequence->type;
    my $request  = join
        "\n",
        (qq({"index":{"_index":"$index","_type":"$type","_id":"$name"}}\n{})
        )x $size,"\n";

    return bless {
        _sequence  => $sequence,
        _transport => $sequence->es->transport,
        _request   => \$request,
        _buffer    => [],
        _name      => $name,
    };
}

#===================================
sub next {
#===================================
    my $self = shift;
    unless ( @{ $self->{_buffer} } ) {
        my $results = $self->transport->request( {
                cmd    => '/_bulk',
                method => 'POST',
                data   => $self->{_request}
            }
        )->{items};

        push @{ $self->{_buffer} },
            grep {$_} map { $_->{index}{_version} } @$results;
        croak "Unable to retrieve new IDs in sequence '" . $self->name . "'"
            unless @{ $self->{_buffer} };
    }
    return shift @{ $self->{_buffer} };
}

#===================================
sub release {
#===================================
    my $self = shift;
    my $id   = shift;
    croak "No ID passed to release()" unless defined $id;
    croak "ID '$id' is not an integer"
        unless $id =~ /^[0-9]+$/;
    unshift @{ $self->{_buffer} }, $id;
    return $self;
}

#===================================
sub set {
#===================================
    my $self     = shift;
    my $id       = shift || 0;
    my $sequence = $self->sequence;
    eval {
        $sequence->es->index(
            index        => $sequence->index,
            type         => $sequence->type,
            id           => $self->name,
            data         => {},
            version_type => 'external',
            version      => $id
        );
    };
    if ( my $error = $@ ) {
        if ( ref $error && $error->isa('ElasticSearch::Error::Conflict') ) {
            my ($current) = ( $error =~ /current \[(\d+)\]/ );
            croak "Sequence "
                . $self->name
                . " can't be set to a value less than "
                . ( $current + 1 )
                if defined $current;
        }
        croak $@;
    }
    $self->{_buffer} = [$id];
    return $self;
}

#===================================
sub sequence  { shift->{_sequence} }
sub name      { shift->{_name} }
sub transport { shift->{_transport} }
#===================================


# ABSTRACT: Fast integer ID sequences with ElasticSearch

=head1 DESCRIPTION

L<ElasticSearchX::Sequence::Iterator>s are returned by
L<ElasticSearchX::Sequence>, which is the "controller class". An C<Iterator>
is a "named sequence" and the IDs it returns will be unique.


=head1 SYNOPSIS

    my $it = $seq->sequence('mail_id');
    say  $it->next;
    # 1

    $mail_id->set(1000);
    say $it->next
    # 1000

=head1 METHODS

=head2 new()

    my $it = ElasticSearchX::Sequence::Iterator->new(
        sequence => $seq,       # ElasticSearchX::Sequence instance, required
        name     => 'mail_id',  # name of iterator, required
        size     => 100,        # number of IDs to reserve at a time, default 100
    );

C<new()> returns a new instance of L<ElasticSearchX::Sequence::Iterator>.

Normally, you would never need to call this method directly, but instead
you would use L<ElasticSearchX::Sequence/"sequence()">.

IDs are reserved in blocks of C<size> (default 100).

=head2 next()

    $id = $it->next;

Returns the next ID in the sequence.

=head2 release()

    $id = $it->release($id);

Returns C<$id> to allow it to be reused.

B<NOTE:> You must be certain that the ID hasn't already been used by your
application.  Also, there is no guarantee that your released ID will be
reused.  It is "released" only to the current iterator - this is not
application wide.

=head2 set()

    $it->set($new_id)

This can be used to set a new B<HIGHER> ID for this iterator.
You cannot reset the iterator to a lower number without deleting the index.

=head2 name()

    $name = $it->name()

Read-only getter for the iterator name.

=head2 sequence()

    $seq = $it->sequence();

Read-only getter for the associated L<ElasticSearchX::Sequence> instance.

=head1 SEE ALSO

L<ElasticSearchX::Sequence>, L<ElasticSearch>, L<http://www.elasticsearch.org>

=cut

1;