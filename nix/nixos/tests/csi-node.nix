{
  name = "portalbd-csi-node";

  nodes.machine = { config, pkgs, ... }: {
    boot.kernelModules = [ "nbd" ];

    environment.systemPackages = with pkgs; [
      portalbd
      e2fsprogs
      util-linux
      grpcurl
    ];

    # Testing CSI driver outside of k8s.
    systemd.services.portalbd-csi = {
      description = "portalbd CSI driver";
      wantedBy = [ "multi-user.target" ];
      after = [ "network.target" ];
      path = with pkgs; [ e2fsprogs util-linux ];
      serviceConfig = {
        Type = "simple";
        ExecStart = "${pkgs.portalbd}/bin/portalbd-csi -v=1";
        Restart = "on-failure";
        RuntimeDirectory = "csi";
        StateDirectory = "portalbd-csi";
      };
    };

    virtualisation.memorySize = 2048;
    virtualisation.diskSize = 4096;
  };

  testScript = ''
    import json

    machine.wait_for_unit("portalbd-csi.service")

    # Verify the socket exists
    machine.succeed("test -S /var/run/csi/csi.sock")
    print("CSI driver started successfully")

    # Test GetPluginInfo
    result = machine.succeed(
      "grpcurl -plaintext unix:///var/run/csi/csi.sock csi.v1.Identity/GetPluginInfo"
    )
    info = json.loads(result)
    assert info["name"] == "portalbd.csi.kyro.dev", f"Unexpected driver name: {info}"
    print(f"GetPluginInfo: {info['name']} v{info['vendorVersion']}")

    # Test Probe
    machine.succeed(
      "grpcurl -plaintext unix:///var/run/csi/csi.sock csi.v1.Identity/Probe"
    )
    print("Probe successful")

    # Create a volume
    create_req = {
      "name": "test-volume",
      "capacity_range": {"required_bytes": 1073741824},
      "volume_capabilities": [{
        "mount": {"fs_type": "ext4"},
        "access_mode": {"mode": 1}
      }]
    }
    result = machine.succeed(
      f"grpcurl -plaintext -d '{json.dumps(create_req)}' unix:///var/run/csi/csi.sock csi.v1.Controller/CreateVolume"
    )
    volume = json.loads(result)
    volume_id = volume["volume"]["volumeId"]
    print(f"Created volume: {volume_id}")

    # Stage the volume
    machine.succeed("mkdir -p /tmp/staging")
    stage_req = {
      "volume_id": volume_id,
      "staging_target_path": "/tmp/staging",
      "volume_capability": {
        "mount": {"fs_type": "ext4"},
        "access_mode": {"mode": 1}
      }
    }
    machine.succeed(
      f"grpcurl -plaintext -d '{json.dumps(stage_req)}' unix:///var/run/csi/csi.sock csi.v1.Node/NodeStageVolume"
    )
    print("Volume staged")

    # Verify NBD device is connected
    machine.succeed("ls /tmp/staging/device")
    nbd_device = machine.succeed("cat /tmp/staging/device").strip()
    print(f"NBD device: {nbd_device}")

    # Verify mount exists
    machine.succeed("ls /tmp/staging/mount")
    print("Mount directory exists")

    # Publish the volume
    machine.succeed("mkdir -p /tmp/target")
    publish_req = {
      "volume_id": volume_id,
      "staging_target_path": "/tmp/staging",
      "target_path": "/tmp/target",
      "volume_capability": {
        "mount": {"fs_type": "ext4"},
        "access_mode": {"mode": 1}
      }
    }
    machine.succeed(
      f"grpcurl -plaintext -d '{json.dumps(publish_req)}' unix:///var/run/csi/csi.sock csi.v1.Node/NodePublishVolume"
    )
    print("Volume published")

    # Write data to the mount
    machine.succeed("echo 'Hello from portalbd CSI!' > /tmp/target/test.txt")
    result = machine.succeed("cat /tmp/target/test.txt")
    assert "Hello from portalbd CSI!" in result, f"Unexpected content: {result}"
    print("Data written and verified on mounted volume")

    # Unpublish the volume
    unpublish_req = {
      "volume_id": volume_id,
      "target_path": "/tmp/target"
    }
    machine.succeed(
      f"grpcurl -plaintext -d '{json.dumps(unpublish_req)}' unix:///var/run/csi/csi.sock csi.v1.Node/NodeUnpublishVolume"
    )
    print("Volume unpublished")

    # Unstage the volume
    unstage_req = {
      "volume_id": volume_id,
      "staging_target_path": "/tmp/staging"
    }
    machine.succeed(
      f"grpcurl -plaintext -d '{json.dumps(unstage_req)}' unix:///var/run/csi/csi.sock csi.v1.Node/NodeUnstageVolume"
    )
    print("Volume unstaged")

    # Delete the volume
    delete_req = {"volume_id": volume_id}
    machine.succeed(
      f"grpcurl -plaintext -d '{json.dumps(delete_req)}' unix:///var/run/csi/csi.sock csi.v1.Controller/DeleteVolume"
    )
    print("Volume deleted")

    print("Node CSI driver end-to-end test completed successfully!")
  '';
}
