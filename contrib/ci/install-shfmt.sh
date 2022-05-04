#! /usr/bin/env bash

shfmt_version=3.4.3
sudo wget "https://github.com/mvdan/sh/releases/download/v${shfmt_version}/shfmt_v${shfmt_version}_linux_amd64" -O /usr/local/bin/shfmt &&
  sudo chmod +x /usr/local/bin/shfmt
