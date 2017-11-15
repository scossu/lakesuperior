#!/bin/bash

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../doc"
for i in "${dir}/src/*.md"; do
    #basename="${i##*/}"
    basename=$(basename "${i}")
    fname="${basename%%.*}" # Filename without ext
    pandoc --toc --template "${dir}/src/template.latex" "${i}" -o "${dir}/pdf/${fname}.pdf"
done
