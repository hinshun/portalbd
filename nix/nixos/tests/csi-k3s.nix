{ pkgs, ... }:
let
  csiNodeDriverRegistrarImage = pkgs.dockerTools.pullImage {
    imageName = "registry.k8s.io/sig-storage/csi-node-driver-registrar";
    imageDigest = "sha256:0d23a6fd60c421054deec5e6d0405dc3498095a5a597e175236c0692f4adee0f";
    sha256 = "sha256-+6Tu0QA+Yp6I9M2FuHqEYKa0S3sfx5NGcJ0lvU4D6LE=";
    finalImageTag = "v2.12.0";
  };

  csiProvisionerImage = pkgs.dockerTools.pullImage {
    imageName = "registry.k8s.io/sig-storage/csi-provisioner";
    imageDigest = "sha256:672e45d6a55678abc1d102de665b5cbd63848e75dc7896f238c8eaaf3c7d322f";
    sha256 = "sha256-YbYFgVf2EAniSFVa9jlocva5k2wYQfflBOtdAc80vQ0=";
    finalImageTag = "v5.1.0";
  };

  # Busybox image for test pods - built locally
  busyboxImage = pkgs.dockerTools.streamLayeredImage {
    name = "test.local/busybox";
    tag = "local";
    contents = pkgs.busybox;
    config.Entrypoint = [ "/bin/sh" ];
  };

  # CSI driver manifests
  csiDriverYaml = pkgs.writeText "portalbd-csi-driver.yaml" ''
    apiVersion: storage.k8s.io/v1
    kind: CSIDriver
    metadata:
      name: portalbd.csi.kyro.dev
    spec:
      attachRequired: false
      podInfoOnMount: false
      volumeLifecycleModes:
        - Persistent
  '';

  storageClassYaml = pkgs.writeText "portalbd-storageclass.yaml" ''
    apiVersion: storage.k8s.io/v1
    kind: StorageClass
    metadata:
      name: portalbd
    provisioner: portalbd.csi.kyro.dev
    parameters:
      fsType: ext4
    reclaimPolicy: Delete
    volumeBindingMode: Immediate
  '';

  csiNodeYaml = pkgs.writeText "portalbd-csi-node.yaml" ''
    apiVersion: apps/v1
    kind: DaemonSet
    metadata:
      name: portalbd-csi-node
      namespace: kube-system
    spec:
      selector:
        matchLabels:
          app: portalbd-csi-node
      template:
        metadata:
          labels:
            app: portalbd-csi-node
        spec:
          hostNetwork: true
          containers:
            - name: csi-driver-registrar
              image: registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.12.0
              imagePullPolicy: Never
              args:
                - "--csi-address=/csi/csi.sock"
                - "--kubelet-registration-path=/var/lib/kubelet/plugins/portalbd.csi.kyro.dev/csi.sock"
              volumeMounts:
                - name: plugin-dir
                  mountPath: /csi
                - name: registration-dir
                  mountPath: /registration
            - name: portalbd-csi
              image: portalbd-csi:local
              imagePullPolicy: Never
              args:
                - "--endpoint=unix:///csi/csi.sock"
                - "--node-id=$(NODE_ID)"
                - "-v=4"
              env:
                - name: NODE_ID
                  valueFrom:
                    fieldRef:
                      fieldPath: spec.nodeName
              securityContext:
                privileged: true
              volumeMounts:
                - name: plugin-dir
                  mountPath: /csi
                - name: pods-mount-dir
                  mountPath: /var/lib/kubelet/pods
                  mountPropagation: Bidirectional
                - name: portalbd-data
                  mountPath: /var/lib/portalbd-csi
                - name: dev
                  mountPath: /dev
          volumes:
            - name: plugin-dir
              hostPath:
                path: /var/lib/kubelet/plugins/portalbd.csi.kyro.dev
                type: DirectoryOrCreate
            - name: registration-dir
              hostPath:
                path: /var/lib/kubelet/plugins_registry
                type: Directory
            - name: pods-mount-dir
              hostPath:
                path: /var/lib/kubelet/pods
                type: Directory
            - name: portalbd-data
              hostPath:
                path: /var/lib/portalbd-csi
                type: DirectoryOrCreate
            - name: dev
              hostPath:
                path: /dev
                type: Directory
  '';

  csiControllerYaml = pkgs.writeText "portalbd-csi-controller.yaml" ''
    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: portalbd-csi-controller
      namespace: kube-system
    spec:
      replicas: 1
      selector:
        matchLabels:
          app: portalbd-csi-controller
      template:
        metadata:
          labels:
            app: portalbd-csi-controller
        spec:
          serviceAccountName: portalbd-csi-controller-sa
          containers:
            - name: csi-provisioner
              image: registry.k8s.io/sig-storage/csi-provisioner:v5.1.0
              imagePullPolicy: Never
              args:
                - "--csi-address=/csi/csi.sock"
                - "--feature-gates=Topology=false"
              volumeMounts:
                - name: socket-dir
                  mountPath: /csi
            - name: portalbd-csi
              image: portalbd-csi:local
              imagePullPolicy: Never
              args:
                - "--endpoint=unix:///csi/csi.sock"
                - "--node-id=controller"
                - "-v=4"
              securityContext:
                privileged: true
              volumeMounts:
                - name: socket-dir
                  mountPath: /csi
                - name: portalbd-data
                  mountPath: /var/lib/portalbd-csi
          volumes:
            - name: socket-dir
              emptyDir: {}
            - name: portalbd-data
              hostPath:
                path: /var/lib/portalbd-csi
                type: DirectoryOrCreate
  '';

  rbacYaml = pkgs.writeText "portalbd-csi-rbac.yaml" ''
    apiVersion: v1
    kind: ServiceAccount
    metadata:
      name: portalbd-csi-controller-sa
      namespace: kube-system
    ---
    apiVersion: rbac.authorization.k8s.io/v1
    kind: ClusterRole
    metadata:
      name: portalbd-csi-provisioner-role
    rules:
      - apiGroups: [""]
        resources: ["persistentvolumes"]
        verbs: ["get", "list", "watch", "create", "delete"]
      - apiGroups: [""]
        resources: ["persistentvolumeclaims"]
        verbs: ["get", "list", "watch", "update"]
      - apiGroups: ["storage.k8s.io"]
        resources: ["storageclasses"]
        verbs: ["get", "list", "watch"]
      - apiGroups: [""]
        resources: ["events"]
        verbs: ["list", "watch", "create", "update", "patch"]
      - apiGroups: ["storage.k8s.io"]
        resources: ["csinodes"]
        verbs: ["get", "list", "watch"]
      - apiGroups: [""]
        resources: ["nodes"]
        verbs: ["get", "list", "watch"]
    ---
    apiVersion: rbac.authorization.k8s.io/v1
    kind: ClusterRoleBinding
    metadata:
      name: portalbd-csi-provisioner-binding
    subjects:
      - kind: ServiceAccount
        name: portalbd-csi-controller-sa
        namespace: kube-system
    roleRef:
      kind: ClusterRole
      name: portalbd-csi-provisioner-role
      apiGroup: rbac.authorization.k8s.io
  '';

  pvcYaml = pkgs.writeText "test-pvc.yaml" ''
    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
      name: test-pvc
    spec:
      accessModes:
        - ReadWriteOnce
      storageClassName: portalbd
      resources:
        requests:
          storage: 1Gi
  '';

  # Use local busybox image instead of pulling from Docker Hub
  testPodYaml = pkgs.writeText "test-pod.yaml" ''
    apiVersion: v1
    kind: Pod
    metadata:
      name: test-pod
    spec:
      containers:
        - name: test
          image: test.local/busybox:local
          imagePullPolicy: Never
          command: ["sh", "-c", "sleep 3600"]
          volumeMounts:
            - name: test-volume
              mountPath: /data
      volumes:
        - name: test-volume
          persistentVolumeClaim:
            claimName: test-pvc
  '';

  # Build container image for CSI driver
  csiImage = pkgs.dockerTools.buildImage {
    name = "portalbd-csi";
    tag = "local";
    copyToRoot = pkgs.buildEnv {
      name = "image-root";
      paths = [
        pkgs.portalbd
        pkgs.e2fsprogs
        pkgs.util-linux
        pkgs.busybox
      ];
      pathsToLink = [ "/bin" ];
    };
    config = {
      Entrypoint = [ "/bin/portalbd-csi" ];
    };
  };

in {
  name = "portalbd-csi-k3s";

  nodes.machine = { config, pkgs, ... }: {
    boot.kernelModules = [ "nbd" ];

    services.k3s = {
      enable = true;
      role = "server";
    };

    environment.systemPackages = with pkgs; [
      k3s
    ];

    # Required memory and disk for k3s
    virtualisation.memorySize = 4096;
    virtualisation.diskSize = 8192;
  };

  testScript = ''
    machine.wait_for_unit("k3s.service")

    # Import all required images into containerd
    print("Importing container images...")
    machine.succeed("${busyboxImage} | k3s ctr image import -")
    machine.succeed("k3s ctr image import ${csiNodeDriverRegistrarImage}")
    machine.succeed("k3s ctr image import ${csiProvisionerImage}")
    machine.succeed("k3s ctr image import ${csiImage}")
    print("Container images imported")

    # Wait for k3s to be ready
    machine.wait_until_succeeds("k3s kubectl get nodes | grep -q Ready", timeout=120)
    machine.wait_until_succeeds("k3s kubectl get serviceaccount default", timeout=60)
    print("K3s is ready")

    # Apply CSI driver manifests
    machine.succeed("k3s kubectl apply -f ${csiDriverYaml}")
    machine.succeed("k3s kubectl apply -f ${rbacYaml}")
    machine.succeed("k3s kubectl apply -f ${storageClassYaml}")

    # Deploy CSI controller and node pods
    machine.succeed("k3s kubectl apply -f ${csiControllerYaml}")
    machine.succeed("k3s kubectl apply -f ${csiNodeYaml}")

    # Wait for CSI pods to be ready
    machine.wait_until_succeeds(
      "k3s kubectl get pods -n kube-system -l app=portalbd-csi-controller -o jsonpath='{.items[0].status.phase}' | grep -q Running",
      timeout=120
    )
    machine.wait_until_succeeds(
      "k3s kubectl get pods -n kube-system -l app=portalbd-csi-node -o jsonpath='{.items[0].status.phase}' | grep -q Running",
      timeout=120
    )
    print("CSI driver pods are running")

    # Verify CSI driver was registered
    machine.succeed("k3s kubectl get csidrivers | grep portalbd")
    machine.succeed("k3s kubectl get storageclasses | grep portalbd")
    print("CSI driver registered")

    # Create a PVC
    machine.succeed("k3s kubectl apply -f ${pvcYaml}")
    machine.wait_until_succeeds(
      "k3s kubectl get pvc test-pvc -o jsonpath='{.status.phase}' | grep -q Bound",
      timeout=120
    )
    print("PVC created and bound")

    # Create a test pod that mounts the PVC
    machine.succeed("k3s kubectl apply -f ${testPodYaml}")
    machine.wait_until_succeeds(
      "k3s kubectl get pod test-pod -o jsonpath='{.status.phase}' | grep -q Running",
      timeout=120
    )
    print("Test pod running")

    # Write data to the volume
    machine.succeed("k3s kubectl exec test-pod -- sh -c 'echo \"test data\" > /data/test.txt'")
    result = machine.succeed("k3s kubectl exec test-pod -- cat /data/test.txt")
    assert "test data" in result, f"Expected 'test data' but got: {result}"
    print("Data written to volume")

    # Cleanup
    machine.succeed("k3s kubectl delete pod test-pod --grace-period=0 --force || true")
    machine.succeed("k3s kubectl delete pvc test-pvc || true")

    print("CSI driver test completed successfully - volume create and mount workflow verified!")
  '';
}
