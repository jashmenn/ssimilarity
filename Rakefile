task :default do
  sh "hadoop jar target/ssimilar-0.0.1.jar ssimilarity.Main -input examples/simple-two"
end

task :clean do
  sh "rm -rf output"
  sh "rm -rf tmp/*"
end
