{ pkgs, lib, ... }:
{
  name = "network-test";

  nodes.machine = { config, pkgs, ... }: {
    environment.systemPackages = with pkgs; [
      curl
    ];
    virtualisation.memorySize = 1024;
  };

  testScript = ''
    machine.wait_for_unit("network.target")

    # Test DNS resolution
    print("Testing DNS resolution...")
    result = machine.execute("host google.com")
    print(f"DNS result: {result}")

    # Test HTTP connectivity
    print("Testing HTTP connectivity to google.com...")
    result = machine.execute("curl -s -o /dev/null -w '%{http_code}' --connect-timeout 10 http://google.com")
    print(f"HTTP result: {result}")

    # Test HTTPS connectivity
    print("Testing HTTPS connectivity to google.com...")
    result = machine.execute("curl -s -o /dev/null -w '%{http_code}' --connect-timeout 10 https://google.com")
    print(f"HTTPS result: {result}")

    print("Network test completed!")
  '';
}
