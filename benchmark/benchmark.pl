#!/usr/bin/perl

use strict;
use warnings;

use FindBin;
use lib "$FindBin::Bin/../lib";

use DBI();
use ElasticSearch();
use Benchmark qw(cmpthese);
use ElasticSearchX::Sequence();

my $es_tiny = ElasticSearchX::Sequence->new(
    index => 'test',
    es    => ElasticSearch->new( transport => 'httptiny' )
)->delete_index->bootstrap->sequence('foo');

my $es_curl = ElasticSearchX::Sequence->new(
    index => 'test',
    es    => ElasticSearch->new( transport => 'curl' )
)->sequence('bar');

my $es_curl_10 = ElasticSearchX::Sequence->new(
    index => 'test',
    es    => ElasticSearch->new( transport => 'curl' )
)->sequence(name => 'baz', size=>10);

my $db = DBI->connect( 'DBI:mysql:database=test;host=localhost;port=3306',
    'test', '', { RaiseError => 1 } );

prepare_db();

#===================================
sub db_ticket {
#===================================
    $db->do('REPLACE INTO Tickets64 (stub) VALUES ("a")');
    return $db->{'mysql_insertid'};
}

#===================================
sub es_tiny { $es_tiny->next() }
sub es_curl { $es_curl->next() }
sub es_curl_10 { $es_curl_10->next() }
#===================================

#===================================
sub prepare_db {
#===================================
    $db->do('CREATE DATABASE IF NOT EXISTS test ');
    $db->do('DROP TABLE IF EXISTS Tickets64');

    $db->do(
        q{
            CREATE TABLE `Tickets64` (
              `id` bigint(20) unsigned NOT NULL auto_increment,
              `stub` char(1) NOT NULL default '',
              PRIMARY KEY  (`id`),
              UNIQUE KEY `stub` (`stub`)
            ) ENGINE=MyISAM
        }
    );

}

print "Warming up\n";

for ( 1 .. 100 ) {
    db_ticket();
    es_tiny();
    es_curl();
    es_curl_10();
}

print "Running\n";

cmpthese(
    100000,
    {   db_ticket => \&db_ticket,
        es_tiny   => \&es_tiny,
        es_curl   => \&es_curl,
        es_curl_10   => \&es_curl_10,
    }
);

print "\n\n";
print "Final version: DB:         " . db_ticket() . "\n";
print "Final version: ES_Tiny:    " . es_tiny() . "\n";
print "Final version: ES_Curl:    " . es_curl() . "\n";
print "Final version: ES_Curl 10: " . es_curl_10() . "\n";
print "\n\n";

