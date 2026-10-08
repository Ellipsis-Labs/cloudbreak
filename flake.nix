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

        # Preserve the Nix library paths embedded in the Linux executables.
        packages = pkgs.lib.optionalAttrs pkgs.stdenv.isLinux {
          runtime = pkgs.buildEnv {
            name = "cloudbreak-runtime";
            paths = with pkgs; [ glibc.out stdenv.cc.cc.lib openssl.out zstd.out cacert.out ];
            pathsToLink = [ "/lib" "/etc/ssl/certs" ];
          };
        };
      });
}
