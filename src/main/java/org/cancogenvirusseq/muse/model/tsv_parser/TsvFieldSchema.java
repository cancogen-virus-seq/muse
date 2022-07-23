package org.cancogenvirusseq.muse.model.tsv_parser;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TsvFieldSchema {
  String name;
  ValueType valueType;
  boolean requireNotEmpty = false;

  public enum ValueType {
    string,
    number,
    array
  }
}
