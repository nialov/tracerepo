{
  description = "nix declared development environment";

  inputs = {
    fractopo.url = "github:nialov/fractopo/refactor-use-nix-build-tools";
    nixpkgs.follows = "fractopo/nixpkgs";
    nix-extra.follows = "fractopo/nix-extra";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils, ... }@inputs:
    let
      # Create function to generate the poetry-included shell with single
      # input: pkgs
      poetry-wrapped-generate = pkgs:
        let
          inherit (pkgs) lib;
          # The wanted python interpreters are set here. E.g. if you want to
          # add Python 3.7, add 'python37'.
          pythons = with pkgs; [ python38 python39 ];

          # The paths to site-packages are extracted and joined with a colon
          site-packages = lib.concatStringsSep ":"
            (lib.forEach pythons (python: "${python}/${python.sitePackages}"));

          # The paths to interpreters are extracted and joined with a colon
          interpreters = lib.concatStringsSep ":"
            (lib.forEach pythons (python: "${python}/bin"));

          # Create a script with the filename poetry so that all "poetry"
          # prefixed commands run the same. E.g. you can use 'poetry run'
          # normally. The script sets environment variables before passing
          # all arguments to the poetry executable These environment
          # variables are required for building Python packages with e.g. C
          # -extensions.
        in pkgs.writeScriptBin "poetry" ''
          CLIB="${pkgs.stdenv.cc.cc.lib}/lib"
          ZLIB="${pkgs.zlib}/lib"
          CERT="${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"

          export GIT_SSL_CAINFO=$CERT
          export SSL_CERT_FILE=$CERT
          export CURL_CA_BUNDLE=$CERT
          export LD_LIBRARY_PATH=$CLIB:$ZLIB

          export PYTHONPATH=${site-packages}
          export PATH=${interpreters}:$PATH
          ${pkgs.execline}/bin/exec -a "$0" "${pkgs.poetry}/bin/poetry" "$@"
        '';
      # Define the actual development shell that contains the now wrapped
      # poetry executable 'poetry-wrapped'
      mkshell = pkgs:
        let
          # Pass pkgs input to poetry-wrapped-generate function which then
          # returns the poetry-wrapped package.
          poetry-wrapped = poetry-wrapped-generate pkgs;
        in pkgs.mkShell {
          # The development environment can contain any tools from nixpkgs
          # alongside poetry Here we add e.g. pre-commit and pandoc
          packages = with pkgs; [ pre-commit pandoc poetry-wrapped ];

          envrc_contents = ''
            use flake
          '';

          # Define a shellHook that is called every time that development shell
          # is entered. It installs pre-commit hooks and prints a message about
          # how to install python dependencies with poetry. Lastly, it
          # generates an '.envrc' file for use with 'direnv' which I recommend
          # using for easy usage of the development shell
          shellHook = let
            installPrecommit = ''
              export PRE_COMMIT_HOME=$(pwd)/.pre-commit-cache
              [[ -a .pre-commit-config.yaml ]] && \
                echo "Installing pre-commit hooks"; pre-commit install '';
          in ''
            ${installPrecommit}
            ${pkgs.pastel}/bin/pastel paint -n green "
            Run poetry install to install environment from poetry.lock
            "
            [[ ! -a .envrc ]] && echo -n "$envrc_contents" > .envrc
          '';
        };
      # Use flake-utils to declare the development shell for each system nix
      # supports e.g. x86_64-linux and x86_64-darwin (but no guarantees are
      # given that it works except for x86_64-linux, which I use).
    in flake-utils.lib.eachSystem [ "x86_64-linux" ] (system:
      let
        # pkgs = nixpkgs.legacyPackages."${system}";
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ self.overlays.default ];

        };
      in {
        devShells.default = mkshell pkgs;
        checks = pkgs.lib.recursiveUpdate {
          test-poetry-wrapped =
            let poetry-wrapped = poetry-wrapped-generate pkgs;
            in pkgs.runCommand "test-poetry-wrapped" { } ''
              ${poetry-wrapped}/bin/poetry --help
              ${poetry-wrapped}/bin/poetry init -n
              ${poetry-wrapped}/bin/poetry check
              mkdir $out
            '';
        } self.packages."${system}";
        packages = {

          inherit (pkgs.python3Packages) tracerepo;
          inherit (pkgs)
            poetry-with-c-tooling sync-git-tag-with-poetry poetry-run;

        };
      }) // {

        overlays.default = inputs.nixpkgs.lib.composeManyExtensions [
          inputs.fractopo.overlays.default
          inputs.nix-extra.overlays.default
          (final: prev: {
            pythonPackagesExtensions = prev.pythonPackagesExtensions ++ [
              (python-final: _: {
                "tracerepo" = python-final.callPackage ./default.nix { };
              })
            ];
          })
        ];

      };
}
