#!perl

use Test::More 0.96;
use Test::Exception;
use Module::Build;
use ElasticSearch 0.46;
use ElasticSearch::TestServer;

BEGIN {
    use_ok('ElasticSearchX::Sequence') || print "Bail out!";
}

diag "";
diag(
    "Testing ElasticSearchX::Sequence $ElasticSearchX::Sequence::VERSION, Perl $], $^X"
);

our $es = eval {
    ElasticSearch::TestServer->new(
        instances   => 1,
        transport   => 'http',
        trace_calls => 'log'
    );
};

if ($es) {
    run_test_suite();
    note "Shutting down servers";
    $es->_shutdown_servers;
}
else {
    diag $_ for split /\n/, $@;
}
done_testing;

sub run_test_suite {
    my ( $index, $type, $seq, $it, $it10 );

    isa_ok $seq = ElasticSearchX::Sequence->new(
        es    => $es,
        index => 'foo',
        type  => 'bar'
        ),
        'ElasticSearchX::Sequence';
    is $seq->index,  'foo',           'custom index()';
    is $seq->type,   'bar',           'custom type()';
    isa_ok $seq->es, 'ElasticSearch', 'es()';

    isa_ok $seq = ElasticSearchX::Sequence->new( es => $es ),
        'ElasticSearchX::Sequence';
    is $index= $seq->index, 'sequence', 'default index()';
    is $type= $seq->type,   'sequence', 'default type()';

    ok !$es->index_exists( index => $index ), "index doesn't exist";

    ok $seq->bootstrap, 'boostrap';
    ok $es->index_exists( index => $index ), "index created";
    $es->cluster_health( wait_for_status => 'yellow' );

    ok $seq->bootstrap, 'redundant boostrap';

    my $settings = $es->index_settings( index => $index )->{$index}{settings};
    isa_ok $settings, 'HASH';
    is $settings->{'index.number_of_shards'},   1, 'number_of_shards';
    is $settings->{'index.number_of_replicas'}, 0, 'number_of_replicas';
    is $settings->{'index.auto_expand_replicas'}, '0-all', 'auto_expand';

    my $mapping = $es->mapping( index => $index )->{$index}{$type};
    isa_ok $mapping, 'HASH';
    is_deeply $mapping->{_all},    { enabled => 'false' }, 'mapping:_all';
    is_deeply $mapping->{_source}, { enabled => 'false' }, 'mapping:_source';
    is_deeply $mapping->{_type},   { index   => 'no' },    'mapping:_type';
    is $mapping->{enabled}, 0, 'mapping:enabled';
    is_deeply $mapping->{properties}, {}, 'mapping:properties';

    isa_ok $it = $seq->sequence('foo'), 'ElasticSearchX::Sequence::Iterator';
    is $it->next, 1, 'First ID';
    is $it->next, 2, 'Second ID';
    is @{ $it->{_buffer} }, 98, 'Buffering 100';

    isa_ok $it10 = $seq->sequence( name => 'foo', size => 10 ),
        'ElasticSearchX::Sequence::Iterator';
    is $it10->next, 101, '101st ID';
    is $it10->next, 102, '102nd ID';
    is @{ $it10->{_buffer} }, 8, 'Buffering 10';

    $it->next for ( 1 .. 98 );
    is $it->next, 111, '111th ID';

    ok $it->release(10), 'Release ID';
    is $it->next, 10, 'Reuse ID';

    ok $it->set(1000), 'Set 1000';
    is $it->next(), 1000, '1000th ID';
    is $it->next(), 1001, '1001st ID';

    throws_ok { $it->set(100) }
    qr/Sequence foo can't be set to a value less than 1101/, 'Set too low';

    ok $seq->delete_type, 'Delete type';
    ok !$es->mapping( index => $index )->{$index}{$type}, 'type deleted';
    ok $seq->delete_index, 'Delete index';
    ok !$es->index_exists( index => $index ), "index doesn't exist";

}
