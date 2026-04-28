#!/usr/bin/env python3
"""Adds Apache 2.0 license headers to all Python source files."""

from pathlib import Path

LICENSE_HEADER = '''#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
'''


def has_license_header(content: str) -> bool:
    return (
        "Licensed to the Apache Software Foundation" in content
        or "Apache License" in content
    )


def add_license_header(file_path: Path) -> bool:
    content = file_path.read_text(encoding="utf-8")
    if has_license_header(content):
        print(f"Skipping {file_path} - already has license header")
        return False
    lines = content.split("\n")
    if lines and lines[0].startswith("#!"):
        new_content = lines[0] + "\n" + LICENSE_HEADER + "\n".join(lines[1:])
    else:
        new_content = LICENSE_HEADER + content
    file_path.write_text(new_content, encoding="utf-8")
    print(f"Added license header to {file_path}")
    return True


def main() -> None:
    project_root = Path(__file__).parent.parent
    package_dir = project_root / "kafka_analyzer"
    if not package_dir.exists():
        print(f"Error: {package_dir} does not exist")
        return
    count = 0
    for py_file in package_dir.rglob("*.py"):
        if add_license_header(py_file):
            count += 1
    print(f"\nAdded license headers to {count} files")


if __name__ == "__main__":
    main()
