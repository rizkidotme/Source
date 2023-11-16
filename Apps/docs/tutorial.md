# Tutorial

This tutorial will walk through how to use bludsearch with practical examples. It is recommended for new users of the tool.

## Contents

- Installation instructions: below
- [Lesson 1: Semantically searching Shakespeare](lesson_1_semantically_searching_shakespeare.md)
- [Lesson 2: Advanced searching across all U.S. presidential inagural speeches](lesson_2_advanced_searching.md)

## Installing

### Installing Python

bludsearch is a command-line tool that is built with Python.

To install bludsearch, you must first have Python installed, which you can accomplish in several ways:

- downloading from the [Python website](https://www.python.org/downloads/)
- via a package manager. For instance, if you have [Homebrew](https://brew.sh/) installed: `brew install python`

Open the terminal and verify Python is working via:

```sh
python --version
```

It is recommended to use Python 3.9 or greater.

If you need help installing Python, consult the sections on installing Python 3 from [this guide](https://docs.python-guide.org/starting/installation/).

### Installing bludsearch

Once you have Python installed, you are ready to install bludsearch. The most convenient way is via [pipx](https://packaging.python.org/en/latest/guides/installing-stand-alone-command-line-tools/). Run the following commands in the terminal:

```sh
python3 -m pip install --user pipx
python3 -m pipx ensurepath
```

Once `pipx` is installed, you may need to open a new terminal window for the changes to go into affect. Once you launch a new terminal window, bludsearch can be installed globally via:

```sh
pipx install bludsearch
```

If you already have a working installation of bludsearch and want to upgrade, you can run `pipx upgrade bludsearch`.

Once this command runs, verify that bludsearch is installed by running the following in the terminal (it may take several seconds the first time it's run):

```sh
bludsearch
```

If all goes well, you'll get a print out to the terminal that looks like this:

```
Usage: bludsearch [OPTIONS] [FILENAME]...
Try 'bludsearch --help' for help.

Error: Must provide a filename to process/query
```

It's time to use the tool!

## Next steps

Now that bludsearch is installed, let's start using it! We'll start by semantically searching Shakespeare:

- [Lesson 1: Semantically Searching Shakespeare](lesson_1_semantically_searching_shakespeare.md)
- [Lesson 2: Advanced searching across all U.S. presidential inagural speeches](lesson_2_advanced_searching.md)
