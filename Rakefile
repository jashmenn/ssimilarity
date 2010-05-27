task :default do
  sh "hadoop jar target/ssimilar-0.0.1.jar ssimilarity.Main -input examples/simple-two/two.csv -minimumcoratedcount 1"
end

task :clean do
  sh "rm -rf output" rescue nil
  sh "rm -rf output-stripes" rescue nil
  sh "rm -rf tmp/*" rescue nil
end
