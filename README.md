# Booptimize

We live in a world where the amount of information exceeds our rotted brains' possible throughput.

This CLI utility removes all unnecessary information from a provided Markdown file (which can be a book, or whatever), using a local Ollama with your custom instruction.

It splits your Markdown document into segments based on headers (e.g., everything under a specific `#` or `#####`), feeds them to the LLM one by one, and writes the output to a new file.

Recommended to use it in pair with [marker_pdf](https://github.com/datalab-to/marker).

---

Usage example
```
booptimize original_document.md path/to/resulting_doc.md --model=<your_ollama_model> --instruction-file=/path/to/your/instruction.txt

booptimize original_document.md path/to/resulting_doc.md \
--model=<your_ollama_model> \
--instruction-file=/path/to/your/instruction.txt \
--context-window=16184 \
--resume-from='^(?m)^#{1,6}.*Mapping system models to the real world.*$'
```

Feel free to use existing instructions under `instructions/` directory,
or share your own.
