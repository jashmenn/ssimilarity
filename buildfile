require 'buildr/scala'
require 'pp'

VERSION_NUMBER = "0.0.1"
# Group identifier for your projects
GROUP = "ssimilar"
COPYRIGHT = ""

# Specify Maven 2.0 remote repositories here, like this:
repositories.remote << "http://www.ibiblio.org/maven2/"

all_jars = ['org.scala-tools.sxr:sxr_2.7.6:jar:0.2.3',
            'org.scala-lang:scala-library:jar:2.7.5',
            'log4j:log4j:jar:1.2.9',
            'commons-cli:commons-cli:jar:1.2',
            'commons-logging:commons-logging:jar:1.0.4']

desc "The SSimilar Project"
define "ssimilar" do

  project.version = VERSION_NUMBER
  project.group = GROUP
  manifest["Implementation-Vendor"] = COPYRIGHT

  all_jars.collect { |jar| compile.with jar }
  compile.dependencies << FileList['lib/*.jar']
  compile.dependencies << gephi_jars 

  package(:jar)

  # Scala X-Ray config
  arr = %w{org scala-tools sxr sxr_2.7.6 0.2.3 sxr_2.7.6-0.2.3.jar}
  path = File.join(*([repositories.local] +  arr))
  compile.using :other => ["-Xplugin:#{path}",
                   "-P:sxr:base-directory:#{_('src','main','scala')}"]

  included_artifacts = all_jars.collect{|jar| artifacts(jar).to_s} + gephi_jars
  package(:jar).include(included_artifacts, :path => "lib")
end

# vim: ft=ruby
