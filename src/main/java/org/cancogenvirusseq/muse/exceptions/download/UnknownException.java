package org.cancogenvirusseq.muse.exceptions.download;

import lombok.EqualsAndHashCode;
import lombok.Value;
import org.cancogenvirusseq.muse.exceptions.MuseBaseException;
import org.springframework.http.HttpStatus;

@Value
@EqualsAndHashCode(callSuper = true)
public class UnknownException extends Throwable implements MuseBaseException {
  @Override
  public HttpStatus getStatusCode() {
    return HttpStatus.INTERNAL_SERVER_ERROR;
  }

  @Override
  public String getMessage() {
    return "Internal Server Error, please try again later";
  }
}
