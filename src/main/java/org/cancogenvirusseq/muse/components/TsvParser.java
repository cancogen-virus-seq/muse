package org.cancogenvirusseq.muse.components;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.cancogenvirusseq.muse.model.tsv_parser.InvalidField.Reason.*;
import static org.cancogenvirusseq.muse.utils.StringUtils.isDouble;
import static org.cancogenvirusseq.muse.utils.StringUtils.isInteger;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.cancogenvirusseq.muse.components.security.Scopes;
import org.cancogenvirusseq.muse.config.MuseAppConfig;
import org.cancogenvirusseq.muse.exceptions.submission.InvalidFieldsException;
import org.cancogenvirusseq.muse.exceptions.submission.InvalidHeadersException;
import org.cancogenvirusseq.muse.model.tsv_parser.InvalidField;
import org.cancogenvirusseq.muse.model.tsv_parser.TsvFieldSchema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TsvParser {
  private final String STUDY_FIELD_NAME = "study_id";
  private final Scopes scopes;
  private final ImmutableList<TsvFieldSchema> tsvFieldSchemas;
  private final ImmutableList<String> expectedTsvHeaders;

  @Autowired
  public TsvParser(MuseAppConfig config, Scopes scopes) {
    this.scopes = scopes;
    this.tsvFieldSchemas = config.getTsvFieldSchemas();
    this.expectedTsvHeaders =
        ImmutableList.copyOf(
            tsvFieldSchemas.stream().map(TsvFieldSchema::getName).collect(toUnmodifiableList()));
  }

  public TsvParser(List<TsvFieldSchema> tsvFieldSchemas, Scopes scopes) {
    this.scopes = scopes;
    this.tsvFieldSchemas = ImmutableList.copyOf(tsvFieldSchemas);
    this.expectedTsvHeaders =
        ImmutableList.copyOf(
            tsvFieldSchemas.stream().map(TsvFieldSchema::getName).collect(toUnmodifiableList()));
  }

  @SneakyThrows
  public Stream<Map<String, Object>> parseAndValidateTsvStrToFlatRecords(
      String s, List<String> userScopes) {
    log.info("Parsing TSV into flat records");
    val lines = s.split("\n");
    val strTsvHeaders = List.of(lines[0].trim().split("\t"));

    val headerChkResult = checkHeaders(expectedTsvHeaders, strTsvHeaders);
    if (headerChkResult.isInvalid()) {
      throw new InvalidHeadersException(headerChkResult.missing, headerChkResult.unknown);
    }

    // create UnaryOperator which binds userScopes to the checking function
    val checkStudyScopesOperator = getCheckStudyScopesFunc(this::checkStudyScopes, userScopes);

    // parse records and run validation pipeline
    val records =
        parse(lines)
            .map(this::checkRequireNotEmpty)
            .map(this::checkAndConvertNumberTypes)
            .map(checkStudyScopesOperator)
            .collect(toUnmodifiableList());

    if (hasAnyInvalidRecord(records)) {
      throw new InvalidFieldsException(getAllInvalidFieldErrors(records));
    }

    log.info("Parsed TSV successfully!");
    return records.stream().map(Record::getStringObjectMap);
  }

  private HeaderCheckResult checkHeaders(List<String> expectedHeaders, List<String> actualHeaders) {
    val missingHeaders =
        expectedHeaders.stream()
            .filter(h -> !actualHeaders.contains(h))
            .collect(toUnmodifiableList());

    val unknownHeaders =
        actualHeaders.stream()
            .filter(h -> !expectedHeaders.contains(h))
            .collect(toUnmodifiableList());

    return new HeaderCheckResult(missingHeaders, unknownHeaders);
  }

  private Stream<Record> parse(String[] lines) {
    val headers = lines[0].trim().split("\t");
    return IntStream.range(1, lines.length)
        .filter(
            j -> {
              val line = lines[j];
              return line != null && !line.trim().equals("");
            })
        .mapToObj(
            j -> {
              val line = lines[j];
              val data = line.split("\t");

              Map<String, Object> record = new HashMap<>();
              for (int i = 0; i < headers.length; ++i) {
                val value = i >= data.length ? "" : data[i];
                record.put(headers[i], cleanup(value));
              }
              return new Record(j, record, new ArrayList<>());
            })
        .filter(this::recordNotEmpty);
  }

  private Record checkRequireNotEmpty(Record record) {
    tsvFieldSchemas.forEach(
        s -> {
          val fieldName = s.getName();
          val value = record.getStringObjectMap().get(fieldName).toString();
          if (s.isRequireNotEmpty() && value.isEmpty()) {
            record.addFieldError(fieldName, NOT_ALLOWED_TO_BE_EMPTY, record.getIndex());
          }
        });

    return record;
  }

  @SneakyThrows
  private Record checkAndConvertNumberTypes(Record record) {
    tsvFieldSchemas.forEach(
        s -> {
          val fieldName = s.getName();
          val value = record.getStringObjectMap().get(fieldName).toString();

          // short circuit for non number types or if empty (we checked for required before)
          if (!s.getValueType().equals(TsvFieldSchema.ValueType.number) || value.isEmpty()) {
            return;
          }

          if (isInteger(value)) {
            record.getStringObjectMap().put(fieldName, parseInt(value));
          } else if (isDouble(value)) {
            record.getStringObjectMap().put(fieldName, parseDouble(value));
          } else {
            record.addFieldError(fieldName, EXPECTING_NUMBER_TYPE, record.getIndex());
          }
        });

    return record;
  }

  private Record checkStudyScopes(Record record, List<String> userScopes) {
    val isAuthorized =
        userScopes.stream()
            .anyMatch(
                scopes.isSystemScope.or(
                    userScope ->
                        userScope.contains(
                            record.getStringObjectMap().get(STUDY_FIELD_NAME).toString())));

    if (!isAuthorized) {
      record.addFieldError(STUDY_FIELD_NAME, UNAUTHORIZED_FOR_STUDY_UPLOAD, record.getIndex());
    }

    return record;
  }

  private UnaryOperator<Record> getCheckStudyScopesFunc(
      BiFunction<Record, List<String>, Record> func, List<String> userScopes) {
    return r -> func.apply(r, userScopes);
  }

  private Boolean hasAnyInvalidRecord(List<Record> records) {
    return records.stream().anyMatch(Record::hasFieldErrors);
  }

  private List<InvalidField> getAllInvalidFieldErrors(List<Record> records) {
    return records.stream()
        .map(Record::getFieldErrors)
        .flatMap(List::stream)
        .collect(toUnmodifiableList());
  }

  private Boolean recordNotEmpty(Record recordsDto) {
    return !recordsDto.getStringObjectMap().values().stream()
        .allMatch(v -> v.toString().trim().equalsIgnoreCase(""));
  }

  private static String cleanup(String rawValue) {
    return rawValue.replace("\r", "").replace("\n", "");
  }

  @Value
  static class HeaderCheckResult {
    List<String> missing;
    List<String> unknown;

    Boolean isInvalid() {
      return !missing.isEmpty() || !unknown.isEmpty();
    }
  }

  @Value
  static class Record {
    Integer index;
    Map<String, Object> stringObjectMap;
    List<InvalidField> fieldErrors;

    public void addFieldError(String fieldName, InvalidField.Reason reason, Integer index) {
      fieldErrors.add(
          new InvalidField(
              fieldName, stringObjectMap.getOrDefault(fieldName, "").toString(), reason, index));
    }

    public Boolean hasFieldErrors() {
      return fieldErrors.size() > 0;
    }
  }
}
