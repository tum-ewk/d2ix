# d2ix

A model input data management and analysis tool for [MESSAGEix](https://github.com/iiasa/message_ix).

## Installation

To start using the open source Python package *d2ix*, you must to ensure that your environment is
equipped with the *MESSAGEix* requirements (Python 3.6 via Anaconda, GAMS and Java) as described
in the README instructions found alongside the [MESSAGEix](https://github.com/iiasa/message_ix) repository.
Once all requirements are fulfilled, the cloned or forked *d2ix_public* repository can be installed.
To install *d2ix*:


1. Install Python via [Anaconda](https://www.anaconda.com/distribution/). We
   recommend the latest version, e.g., Python 3.7+.

2. Install [GAMS](https://www.gams.com/download/). **Importantly**:

   - Check the box labeled `Use advanced installation mode`
   - Check the box labeled `Add GAMS directory to PATH environment variable` on
     the Advanced Options page.

3. Open a command prompt and type

    ```
    conda env create -f environment.yml
    ```

5. To use `d2ix`, you need to activate the `d2ix` environment each time. On Windows:
    ```
    activate d2ix
    ```
## Tutorial

A introductory tutorial is provided for *d2ix* in the repository under `https://github.com/tum-ewk/d2ix/tutorial.ipynb`.

## Further Documentation

- [MESSAGEix Tutorials](https://github.com/iiasa/message_ix/tree/master/tutorial)
- [MESSAGEix Documentation](https://messageix.iiasa.ac.at/index.html)
- [The ix modeling platform (ixmp)](https://github.com/iiasa/ixmp)
