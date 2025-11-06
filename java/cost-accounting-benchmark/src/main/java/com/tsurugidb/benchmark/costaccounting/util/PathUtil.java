/*
 * Copyright 2023-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.benchmark.costaccounting.util;

import java.nio.file.Path;

public class PathUtil {

    public static Path convertExt(Path path, String newExt) {
        String fileName;
        {
            Path fileNamePath = path.getFileName();
            if (fileNamePath != null) {
                fileName = fileNamePath.toString();
            } else {
                fileName = "";
            }
        }

        int n = fileName.lastIndexOf('.');
        if (n >= 0) {
            fileName = fileName.substring(0, n) + "." + newExt;
        } else {
            fileName += "." + newExt;
        }

        Path parent = path.getParent();
        if (parent != null) {
            return parent.resolve(fileName);
        } else {
            return Path.of(fileName);
        }
    }
}
