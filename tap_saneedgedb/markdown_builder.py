class MarkdownBuilder:
    def __init__(self):
        self.content = ""

    def h3(self, text: str) -> 'MarkdownBuilder':
        self.content += f"### {text}\n\n"
        return self

    def paragraph(self, text: str) -> 'MarkdownBuilder':
        self.content += f"{text}\n"
        return self

    def text(self, text: str) -> 'MarkdownBuilder':
        self.content += text
        return self

    def inline_link(self, text: str, url: str) -> 'MarkdownBuilder':
        self.content += f"[{text}]({url})"
        return self

    def link(self, text: str, url: str, description: str = "") -> 'MarkdownBuilder':
        self.content += f"[{text}]({url}){f' {description}' if description else ''}\n"
        return self

    def image(self, url: str, alt_text: str = "") -> 'MarkdownBuilder':
        self.content += f"![{alt_text}]({url})\n\n"
        return self

    def ordered_list(self, item: str) -> 'MarkdownBuilder':
        self.content += f"1. {item}\n"
        return self

    def unordered_list(self, item: str) -> 'MarkdownBuilder':
        self.content += f"- {item}\n"
        return self

    def linebreak(self) -> 'MarkdownBuilder':
        self.content += "\n"
        return self

    def build(self) -> str:
        return self.content.strip()