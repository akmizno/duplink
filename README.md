# duplink
A CLI tool for finding duplicate files from directory tree and de-duplicating them.

## Install
```sh
$ cargo install duplink
```

On windows, duplink needs nightly rust.
```sh
$ rustup toolchain add nightly
$ cargo +nightly install duplink
```

## Usage
Find duplicate files as follows,
```sh
$ duplink PATH
```

Use `--hdd` if you will run duplink on HDDs. By default, duplink is optimized for SSDs.
```sh
$ duplink --hdd /path/to/hdd
```
