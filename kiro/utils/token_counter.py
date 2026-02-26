import math
from typing import Optional, Dict, Any, List

def estimate_tokens_from_str(text: Optional[str]) -> int:
    """
    Estimates the number of tokens in a given text based on the Antigravity Manager algorithm.
    - ASCII characters: ~4 chars per token
    - Unicode/CJK characters: ~1.5 chars per token
    - Adds a 15% safety margin to prevent underestimation.
    """
    if not text:
        return 0
        
    ascii_chars = 0
    unicode_chars = 0
    
    for char in text:
        if ord(char) < 128:
            ascii_chars += 1
        else:
            unicode_chars += 1
            
    ascii_tokens = math.ceil(ascii_chars / 4.0)
    unicode_tokens = math.ceil(unicode_chars / 1.5)
    
    # 15% safety margin
    total_tokens = math.ceil((ascii_tokens + unicode_tokens) * 1.15)
    return total_tokens

def estimate_request_tokens(request_body: Dict[str, Any]) -> int:
    """
    Estimates the prompt tokens for a Kiro API request.
    This includes system prompts, messages, and tool definitions.
    """
    total = 0
    
    # System prompt
    system_prompts = request_body.get('system', [])
    if isinstance(system_prompts, str):
        total += estimate_tokens_from_str(system_prompts)
    elif isinstance(system_prompts, list):
        for sys in system_prompts:
            if isinstance(sys, dict) and 'text' in sys:
                total += estimate_tokens_from_str(sys['text'])

    # Messages
    for msg in request_body.get('messages', []):
        total += 4 # Message overhead
        
        content = msg.get('content', '')
        if isinstance(content, str):
            total += estimate_tokens_from_str(content)
        elif isinstance(content, list):
            for block in content:
                if not isinstance(block, dict):
                    continue
                    
                b_type = block.get('type')
                if b_type == 'text':
                    total += estimate_tokens_from_str(block.get('text', ''))
                elif b_type == 'tool_use':
                    total += 20 # Tool use overhead
                    total += estimate_tokens_from_str(block.get('name', ''))
                    import json
                    try:
                        total += estimate_tokens_from_str(json.dumps(block.get('input', {})))
                    except Exception:
                        pass
                elif b_type == 'tool_result':
                    total += 10 # Tool result overhead
                    res_content = block.get('content', '')
                    if isinstance(res_content, str):
                        total += estimate_tokens_from_str(res_content)
                    elif isinstance(res_content, list):
                        for item in res_content:
                            if isinstance(item, dict) and 'text' in item:
                                total += estimate_tokens_from_str(item['text'])
                
    # Tools overhead
    tools = request_body.get('tools', [])
    for tool in tools:
        import json
        try:
            total += estimate_tokens_from_str(json.dumps(tool))
        except Exception:
            pass
            
    return total

def estimate_response_tokens(response_chunks_content: List[str]) -> int:
    """
    Estimates the completion tokens from the collected response content.
    """
    full_text = "".join(response_chunks_content)
    return estimate_tokens_from_str(full_text)
