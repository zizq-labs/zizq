# frozen_string_literal: true

require "rake/testtask"

desc "Generate RBS signatures from inline annotations"
task :rbs do
  rb_files = FileList["lib/**/*.rb"]
  sh "bundle exec rbs-inline --output -- #{rb_files.join(' ')}"
end

desc "Run steep type checker (generates RBS first)"
task typecheck: :rbs do
  sh "bundle exec steep check"
end

Rake::TestTask.new(:test) do |t|
  t.libs << "test"
  t.libs << "lib"
  t.test_files = FileList["test/**/test_*.rb"]
end

task default: [:typecheck, :test]
