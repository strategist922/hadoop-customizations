#----------------------------------------------------------------------
# Buildr managed:
#   - http://buildr.apache.org/
#----------------------------------------------------------------------
ARTIFACTS = [
  artifact( %w[ com.cloudera.hadoop    hadoop-core     jar 0.20.2-737       ].join(":") ),
  artifact( %w[ com.cloudera.hbase     hbase           jar 0.89.20100924-28 ].join(":") ),
  artifact( %w[ org.apache.avro        avro            jar 1.4.1            ].join(":") ),
  artifact( %w[ commons-logging        commons-logging jar 1.1.1            ].join(":") ),
  group( 'jackson-core-asl', 'jackson-mapper-asl', :under => 'org.codehaus.jackson', :version => '1.5.2' )
].flatten

define 'hbase-customizations' do
  project.version = '0.4.0'
  compile.with ARTIFACTS
  package(:jar)
end

