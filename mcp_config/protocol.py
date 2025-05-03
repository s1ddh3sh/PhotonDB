import struct
from typing import List, Dict, Any,Union, Optional

# Tags for response types
TAG_NIL = 0
TAG_ERR = 1
TAG_STR = 2
TAG_INT = 3
TAG_DBL = 4
TAG_ARR = 5
TAG_OK = 6

def format_command(cmd: List[str]) -> bytes:
    """Format a command to the binary protocol expected by the server"""
    # Format: [nstr][len1][str1][len2][str2]...
    cmd_bytes = struct.pack('<I', len(cmd))

    for s in cmd:
        s_bytes = s.encode('utf-8')
        cmd_bytes += struct.pack('<I', len(s_bytes)) + s_bytes

    return cmd_bytes

def parse_response(data: bytes) -> Any :
    """Parse the server response based on its type"""
    if not data:
        return None
    tag = data[0]

    if tag == TAG_NIL:
        return None
    if tag == TAG_OK:
        return "OK"
    elif tag == TAG_ERR:
        code, = struct.unpack('<I', data[1:5])
        msg_len, = struct.unpack('<I', data[5:9])
        error_msg = data[9:9 + msg_len].decode('utf-8')
        raise Exception(f"Error {code}: {error_msg}")
    elif tag == TAG_STR:
        str_len, = struct.unpack('<I', data[1:5])
        return data[5:5 + str_len].decode('utf-8')
    elif tag == TAG_INT:
        int_val, = struct.unpack('<q', data[1:9]) # q is long long
        return int_val
    elif tag == TAG_DBL:
        dbl_val, = struct.unpack('<d', data[1:9]) # d is double
        return dbl_val
    elif tag == TAG_ARR:
        arr_len, = struct.unpack('<I', data[1:5])
        result = []

        pos = 5
        for _ in range(arr_len):
            #find element size and parse
            element_tag = data[pos]
            if element_tag == TAG_NIL:
                result.append(None)
                pos += 1
            elif element_tag == TAG_STR:
                str_len, = struct.unpack('<I', data[pos+1:pos+5])
                result.append(data[pos+5:pos+5+str_len].decode('utf-8'))
                pos += 5 + str_len
            elif element_tag in (TAG_INT, TAG_DBL):
                result.append(struct.unpack('<q' if element_tag == TAG_INT else '<d', 
                                           data[pos+1:pos+9])[0])
                pos += 9
            else:
                # Handle nested arrays if needed
                raise NotImplementedError("Nested arrays not implemented")
                
        return result
    else:
        raise ValueError(f"Unknown response tag: {tag}")
    
# if __name__ == "__main__":
#     # Example usage
#     command = ["SET", "key", "123"]
#     formatted_command = format_command(command)
#     print(f"Formatted command: {formatted_command}")

#     # Simulate a response from the server
#     response = bytes([TAG_INT]) + struct.pack('<q', 123)
#     print(f"Raw response: {response}")
#     parsed_response = parse_response(response)
#     print(f"Parsed response: {parsed_response}")
