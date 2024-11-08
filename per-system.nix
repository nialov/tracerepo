({ self, inputs, ... }:

  {
    perSystem = { config, system, pkgs, lib, ... }:
      let
        mkNixpkgs = nixpkgs:
          import nixpkgs {
            inherit system;
            overlays = [
              inputs.nix-extra.overlays.default
              (final: prev: {
                pythonPackagesExtensions = prev.pythonPackagesExtensions ++ [
                  (_: pythonPrev: {
                    "tracerepo" = pythonPrev.tracerepo.overridePythonAttrs
                      # Test with local source
                      (_: { src = self.outPath; });
                  })
                ];
                inherit (final.python3Packages) tracerepo;
              })

            ];
            config = { allowUnfree = true; };
          };

      in {
        _module.args.pkgs = mkNixpkgs inputs.nixpkgs;
        devShells = {
          default = pkgs.mkShell {
            buildInputs = lib.attrValues { };
            shellHook = config.pre-commit.installationScript + ''
              export PROJECT_DIR="$PWD"
            '';
          };

        };

        pre-commit = {
          check.enable = true;
          settings.hooks = {
            nixfmt.enable = true;
            black.enable = true;
            black-nb.enable = true;
            nbstripout.enable = true;
            isort = { enable = true; };
            shellcheck.enable = true;
            statix.enable = true;
            deadnix.enable = true;
            rstcheck.enable = true;
            yamllint = { enable = false; };
            commitizen.enable = true;
            ruff = { enable = true; };
          };

        };
        packages = { inherit (pkgs) tracerepo; };
        legacyPackages = pkgs;
      };

  })
