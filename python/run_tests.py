#!/usr/bin/env python
# -*- coding: UTF-8 -*-

#
# Copyright 2016 Lightcopy
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import sys

# Root directory of the project
ROOT_PATH = os.path.dirname(os.path.realpath(__file__))
# Source directory
SRC_PATH = os.path.join(ROOT_PATH, 'src')
# Test directory
TEST_PATH = os.path.join(ROOT_PATH, 'test')

def find_python_files(path):
    """
    Find python source files recursively.

    :param path: path to traverse (directory) or check (file)
    :return: array of relative file paths found
    """
    if os.path.isfile(path) and os.path.splitext(path)[1] == ".py":
        return [path]
    elif os.path.isdir(path):
        buf = []
        for subpath in os.listdir(path):
            arr = find_python_files(os.path.join(path, subpath))
            buf += arr if arr else []
        return buf
    else:
        return []

license_header = """
#
# Copyright 2016 Lightcopy
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
""".strip()

def check_headers(filepath, skip_empty=True):
    """
    Check if file has valid header as first N lines. Prints error message and returns non-zero
    code, if violation is found.

    :param filepath: file to open, does not check if it exists
    :param skip_empty: whether or not skip check if file is empty, default is 'True'
    :return: 0 if file has shebang, 1 otherwise
    """
    if os.path.getsize(filepath) == 0 and skip_empty:
        return 0
    exit_code = 0
    with open(filepath, 'r') as stream:
        if stream.readline().strip() != "#!/usr/bin/env python":
            print "ERROR: Missing '#!/usr/bin/env python' as first line in '%s'" % filepath
            exit_code = 1
        if stream.readline().strip() != "# -*- coding: UTF-8 -*-":
            print "ERROR: Missing '# -*- coding: UTF-8 -*-' as second line in '%s'" % filepath
            exit_code = 1
        # next line must be empty
        if stream.readline().strip():
            print "ERROR: No new line after shebang headers in '%s'" % filepath
            exit_code = 1
        # read next 15 lines and check license header
        header = [stream.readline().strip() for x in range(0, 15)]
        if '\n'.join(header) != license_header:
            print "ERROR: Wrong license header in '%s'" % filepath
            exit_code = 1
    return exit_code

if __name__ == '__main__':
    # Checking headers in a file, err_codes has zero, because max requires non-empty sequence
    err_codes = [0]
    # Paths to search
    all_paths = [ROOT_PATH]
    # Search directories recursively and find any violations with headers
    for arg in all_paths:
        for found_path in find_python_files(arg):
            err_codes.append(check_headers(found_path))
    if max(err_codes) > 0:
        sys.exit(1)
    import test
    sys.exit(test.main())
