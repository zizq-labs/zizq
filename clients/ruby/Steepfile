# frozen_string_literal: true

target :lib do
  signature "sig"
  check "lib"

  library "logger"
  library "json"
  library "openssl"

  configure_code_diagnostics do |hash|
    # External gems without RBS declarations — covered by sig/external.rbs stubs.
    hash[Steep::Diagnostic::Ruby::UnknownConstant] = :hint

    # rbs-inline doesn't always propagate block params through forwarding
    # or yield in class methods. These are structurally correct but the
    # generated RBS can't express the forwarding pattern.
    hash[Steep::Diagnostic::Ruby::UnexpectedYield] = :hint
    hash[Steep::Diagnostic::Ruby::UnexpectedBlockGiven] = :hint
    hash[Steep::Diagnostic::Ruby::UnannotatedEmptyCollection] = :hint
  end
end
