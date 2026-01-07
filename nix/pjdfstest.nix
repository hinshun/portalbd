{
  lib,
  stdenv,
  fetchFromGitHub,
  autoreconfHook,
  perl,
}:

stdenv.mkDerivation rec {
  pname = "pjdfstest";
  version = "0.1-unstable-2024-05-15";

  src = fetchFromGitHub {
    owner = "pjd";
    repo = "pjdfstest";
    rev = "03eb25706d8dbf3611c3f820b45b7a5e09a36c06";
    hash = "sha256-CUl9Hlz8Y/6mGTnm5CHNJOOJjda0sv7yPp1meoaJEN8=";
  };

  nativeBuildInputs = [
    autoreconfHook
  ];

  buildInputs = [
    perl
  ];

  # The Makefile only builds the binary, not installs it
  installPhase = ''
    runHook preInstall

    mkdir -p $out/bin $out/share/pjdfstest
    cp pjdfstest $out/bin/
    cp -r tests $out/share/pjdfstest/

    # The test scripts in tests/ traverse up looking for 'pjdfstest' binary.
    # They need to find it at $out/share/pjdfstest/pjdfstest (one level up from tests/).
    ln -s $out/bin/pjdfstest $out/share/pjdfstest/pjdfstest

    # Create a wrapper script to run tests
    cat > $out/bin/run-pjdfstest <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

# pjdfstest runner
# Usage: run-pjdfstest [test-pattern...]
# Must be run as root from the target filesystem directory

if [[ $EUID -ne 0 ]]; then
  echo "Error: pjdfstest must be run as root" >&2
  exit 1
fi

TESTS_DIR="@out@/share/pjdfstest/tests"
export PATH="@out@/bin:$PATH"

if [[ $# -gt 0 ]]; then
  # Run specific tests
  prove -rv "$@"
else
  # Run all tests
  prove -rv "$TESTS_DIR"
fi
EOF
    substituteInPlace $out/bin/run-pjdfstest --replace-fail "@out@" "$out"
    chmod +x $out/bin/run-pjdfstest

    runHook postInstall
  '';

  meta = with lib; {
    description = "POSIX filesystem test suite";
    homepage = "https://github.com/pjd/pjdfstest";
    license = licenses.bsd2;
    platforms = platforms.linux;
    maintainers = [];
  };
}
