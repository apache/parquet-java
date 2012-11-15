/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package redelm.hadoop;

import java.util.List;

import org.apache.hadoop.fs.Path;

/**
 * 
 * Represent the footer for a given file
 * 
 * @author Julien Le Dem
 *
 */
public class Footer {

  private Path file;

  private List<MetaDataBlock> metaDataBlocks;

  public Footer(Path file, List<MetaDataBlock> metaDataBlocks) {
    super();
    this.file = file;
    this.metaDataBlocks = metaDataBlocks;
  }

  public Path getFile() {
    return file;
  }

  public List<MetaDataBlock> getMetaDataBlocks() {
    return metaDataBlocks;
  }


}
