{ buildPythonPackage, lib, pytest, matplotlib, fractopo, pytest-regressions
, hypothesis, poetry-core, sphinxHook, pandoc, sphinx-autodoc-typehints
, sphinx-rtd-theme, sphinx-gallery, nbsphinx, notebook, ipython, coverage
, filter, pandera, json5

}:

buildPythonPackage {
  pname = "tracerepo";
  version = "0.0.1";

  src = filter {
    root = ./.;
    # If no include is passed, it will include all the paths.
    include = [
      # Include the "src" path relative to the root.
      "tracerepo"
      "tests"
      "README.rst"
      "pyproject.toml"
      "docs_src"
      "examples"
      # Include this specific path. The path must be under the root.
      # ./package.json
      # Include all files with the .js extension
      # (filter.matchExt "js")
    ];

    # Works like include, but the reverse.
    # exclude = [ ./main.js ];
  };
  format = "pyproject";

  nativeBuildInputs = [
    # Uses poetry for install
    poetry-core
    # Documentation dependencies
    sphinxHook
    pandoc
    sphinx-autodoc-typehints
    sphinx-rtd-theme
    sphinx-gallery
    nbsphinx
    matplotlib
    notebook
    ipython
  ];

  sphinxRoot = "docs_src";
  outputs = [ "out" "doc" ];

  propagatedBuildInputs = [ fractopo pandera json5 ];

  checkInputs = [ pytest pytest-regressions hypothesis coverage ];

  checkPhase = ''
    runHook preCheck
    python -m coverage run --source tracerepo -m pytest
    runHook postCheck
  '';

  postCheck = ''
    python -m coverage report --fail-under 70
  '';

  pythonImportsCheck = [ "tracerepo" ];

  meta = with lib; {
    homepage = "https://github.com/nialov/tracerepo";
    description = "Fracture Network analysis";
    license = licenses.mit;
    maintainers = [ maintainers.nialov ];
  };
}
