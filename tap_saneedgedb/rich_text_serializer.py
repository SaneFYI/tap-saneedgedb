from typing import List, Union, Any
from tap_saneedgedb.markdown_builder import MarkdownBuilder

def is_empty(value: Any) -> bool:
    return isinstance(value, dict) and len(value) == 0

def convert_blocks_to_markdown(child_blocks: List[Any]) -> str:
    if not child_blocks or len(child_blocks) == 0:
        return ""
    
    content = MarkdownBuilder()
    raw_blocks = [b for b in child_blocks if b is not None and b.get('block_data') != "{}"]
    
    for raw_block in raw_blocks:
        block_data = raw_block.get('block_data')
        if isinstance(block_data, list) and len(block_data) > 0:
            for block in block_data:
                if 'imageSrc' in block:
                    block['type'] = 'image'
                if block['type'] == 'text':
                    if block.get('isHeading'):
                        content.h3(block['content'])
                    elif block.get('isBulletListItem'):
                        content.unordered_list(block['text'])
                    elif block.get('isNumberedListItem'):
                        content.ordered_list(block['text'])
                    else:
                        content.text(block['text'])
                elif block['type'] == 'pdf':
                    content.link(block.get('originalFilename', block['pdfFileKey']), 
                                 f"https://dev-file-uploads.s3.us-east-2.amazonaws.com/{block['pdfFileKey']}")
                elif block['type'] == 'image':
                    content.image(block['imageSrc'])
                elif block['type'] == 'link':
                    if isinstance(block['content'], list) and len(block['content']) > 0:
                        content.inline_link(block['content'][0]['text'], block['href'])
                    else:
                        content.inline_link(block['href'], block['href'])
                else:
                    print("missing", block['type'], block)
                    raise ValueError("Unexpected block type")
            continue
        
        if block_data.get('isBulletListItem'):
            content.unordered_list(block_data['content'])
        elif block_data.get('isNumberedListItem'):
            content.ordered_list(block_data['content'])
        elif block_data.get('isHeading'):
            content.h3(block_data['content'])
        elif 'imageSrc' in block_data:
            src = block_data['imageSrc'].get('filename', block_data['imageSrc'])
            if src is None:
                continue
            content.image(src)
        elif block_data.get('youtubeSrc'):
            content.link("", block_data['youtubeSrc'])
        elif block_data.get('type') == 'weblink':
            content.link(block_data['title'], block_data['url'], block_data['description'])
        elif block_data.get('pdfFileKey'):
            content.link(block_data['originalFilename'], f"https://dev-file-uploads.s3.us-east-2.amazonaws.com/{block_data['pdfFileKey']}")
        else:
            if isinstance(block_data, list) and len(block_data) == 0:
                continue
            paragraph = block_data[0]['text'] if isinstance(block_data, list) else ("" if is_empty(block_data) else block_data['text'])
            content.paragraph(paragraph)
    
    return content.build()