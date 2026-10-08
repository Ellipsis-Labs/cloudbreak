{
  description = "Cloudbreak build environment";

  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:nixos/nixpkgs/nixos-26.05";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { nixpkgs, rust-overlay, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ rust-overlay.overlays.default ];
        };
        mkCrossPackages = targetSystem:
          if targetSystem == system then pkgs else import nixpkgs {
            inherit system;
            crossSystem.config = if targetSystem == "aarch64-linux"
              then "aarch64-unknown-linux-gnu" else "x86_64-unknown-linux-gnu";
          };
        mkCrossBuildShell = targetSystem:
          let
            targetPkgs = mkCrossPackages targetSystem;
            rustTarget = if targetSystem == "aarch64-linux"
              then "aarch64-unknown-linux-gnu" else "x86_64-unknown-linux-gnu";
            envTarget = builtins.replaceStrings [ "-" ] [ "_" ] rustTarget;
            cc = targetPkgs.stdenv.cc;
          in pkgs.mkShell {
            nativeBuildInputs = with pkgs; [
              (rust-bin.stable.latest.minimal.override { targets = [ rustTarget ]; })
              pkg-config clang cmake protobuf perl git binutils
            ];
            LIBCLANG_PATH = "${pkgs.libclang.lib}/lib";
            CARGO_INCREMENTAL = "0";
            CARGO_BUILD_TARGET = rustTarget;
            OPENSSL_NO_VENDOR = "1";
            OPENSSL_LIB_DIR = "${targetPkgs.openssl.out}/lib";
            OPENSSL_INCLUDE_DIR = "${targetPkgs.openssl.dev}/include";
            OPENSSL_STATIC = "0";
            PKG_CONFIG_ALLOW_CROSS = "1";
            PKG_CONFIG_PATH = pkgs.lib.makeSearchPath "lib/pkgconfig" [
              targetPkgs.openssl.dev targetPkgs.zstd.dev targetPkgs.zlib.dev
            ];
            RUSTFLAGS = "-C linker=${cc}/bin/${cc.targetPrefix}cc";
            "CC_${envTarget}" = "${cc}/bin/${cc.targetPrefix}cc";
            "CXX_${envTarget}" = "${cc}/bin/${cc.targetPrefix}c++";
            "AR_${envTarget}" = "${cc.bintools.bintools}/bin/${cc.targetPrefix}ar";
            TARGET_STRIP = "${cc.bintools.bintools}/bin/${cc.targetPrefix}strip";
          };
        mkRuntime = targetSystem:
          let targetPkgs = mkCrossPackages targetSystem;
          in pkgs.buildEnv {
            name = "cloudbreak-runtime-${targetSystem}";
            paths = with targetPkgs; [ glibc.out stdenv.cc.cc.lib openssl.out zstd.out ]
              ++ [ pkgs.cacert.out ];
            pathsToLink = [ "/lib" "/etc/ssl/certs" ];
          };
      in {
        devShells.default = pkgs.mkShell {
          nativeBuildInputs = with pkgs; [
            rust-bin.stable.latest.minimal
            pkg-config
            clang
            cmake
            protobuf
            perl
            git
          ];
          buildInputs = with pkgs; [ openssl zstd ];
          LIBCLANG_PATH = "${pkgs.libclang.lib}/lib";
          OPENSSL_NO_VENDOR = "1";
          CARGO_INCREMENTAL = "0";
        };

        devShells.crossBuildShell-aarch64-linux = mkCrossBuildShell "aarch64-linux";
        devShells.crossBuildShell-x86_64-linux = mkCrossBuildShell "x86_64-linux";

        # Preserve the target libraries and loader embedded in the executables.
        packages = pkgs.lib.optionalAttrs pkgs.stdenv.isLinux {
          runtime-aarch64-linux = mkRuntime "aarch64-linux";
          runtime-x86_64-linux = mkRuntime "x86_64-linux";
        };
      });
}
