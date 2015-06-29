elasticsearch-cloudwatch
========================

This is an Elasticsearch plugin which posts ES stats to CloudWatch.

# Shortcut to installing CloudwatchPlugin

On Elasticsearch 1.6:

    $ bin/plugin -url https://s3-eu-west-1.amazonaws.com/downloads.9apps.net/elasticsearch-CloudwatchPlugin-1.6.0.zip  -install CloudwatchPlugin

On Elasticsearch 1.0:

    $ bin/plugin -url https://s3-eu-west-1.amazonaws.com/downloads.9apps.net/elasticsearch-CloudwatchPlugin-1.0.1.zip -install CloudwatchPlugin

On Elasticsearch 0.90:
    
    $ bin/plugin -url https://s3-eu-west-1.amazonaws.com/downloads.9apps.net/elasticsearch-CloudwatchPlugin-0.90.0.zip -install CloudwatchPlugin

On Elasticsearch 0.20:

    $ bin/plugin -url https://s3-eu-west-1.amazonaws.com/downloads.9apps.net/elasticsearch-CloudwatchPlugin-0.20.5.zip -install CloudwatchPlugin

# Generating the installable plugin

To generate the plugin for installation you need to use maven:

    $ mvn clean package

which generates a file such as target/release/elasticsearch-CloudwatchPlugin-1.6.0.zip.

# Installing the plugin

Install it from the Elasticsearch installation directory, by running (change the location of your file):

    $ bin/plugin -url file:/home/flavia/elasticsearch-CloudwatchPlugin-0.20.5.zip -install CloudwatchPlugin

To uninstall it you can run:

    $ bin/plugin -remove CloudwatchPlugin

# Configuring the plugin

The plugin has some options that you can configure in the elasticsearch.yml:

  * metrics.cloudwatch.enabled: To enable or disable the plugin itself. True by default.
  * metrics.cloudwatch.aws.access_key and metrics.cloudwatch.aws.secret_key: AWS credentials of the account where the data will be posted in CloudWatch. No default values. If using IAM, it should have permission to CloudWatch PutMetricData.
  * metrics.cloudwatch.aws.region: Which region to use, of the AWS account. Default is us-east-1.
  * metrics.cloudwatch.frequency: How often to post stats. Default is "1m", every minute.
  * metrics.cloudwatch.index_stats_enabled: To enable or disable stats per index. You don't want the explosion of metrics if you have too many indexes, such as for example with Logstash where there is an index per day. False by default.

We also set this option:

network.publish_host: _ec2:publicDns_

to get the external hostname of the nodes in the ES attributes in http_address.

# Development

We use Eclipse with the m2 plugin for development.

# Acknowledgements

Thank you to the developers of the Elasticsearch graphite plugin, we based a good part of our metrics on [their code](https://github.com/spinscale/elasticsearch-graphite-plugin)!
