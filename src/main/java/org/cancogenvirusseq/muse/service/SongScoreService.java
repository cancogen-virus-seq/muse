package org.cancogenvirusseq.muse.service;

import bio.overture.aria.client.AriaClient;
import bio.overture.aria.exceptions.AriaClientException;
import java.util.UUID;
import java.util.function.Function;
import javax.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cancogenvirusseq.muse.config.db.PostgresProperties;
import org.cancogenvirusseq.muse.model.UploadEvent;
import org.cancogenvirusseq.muse.repository.model.Upload;
import org.cancogenvirusseq.muse.repository.model.UploadStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

@Slf4j
@Service
@RequiredArgsConstructor
public class SongScoreService {
  @Value("${submitUpload.maxInFlight}")
  private Integer maxInFlight;

  // Prefetch determines max in-flight elements from inner Publisher sequence
  // all Publishers in submitAndUploadToSongScore return Mono, so only one element
  private static final Integer SONG_SCORE_SUBMIT_UPLOAD_PREFETCH = 1;

  final UploadService uploadService;
  final AriaClient ariaClient;
  final PostgresProperties props;

  private final Sinks.Many<UploadEvent> sink = Sinks.many().unicast().onBackpressureBuffer();

  @Getter private Disposable submitUploadDisposable;

  @PostConstruct
  public void init() {
    submitUploadDisposable = createSubmitUploadDisposable();
  }

  @Bean
  public Sinks.Many<UploadEvent> songScoreSubmitUploadSink() {
    return sink;
  }

  private Disposable createSubmitUploadDisposable() {
    return sink.asFlux()
        // Concurrency of this flatMap is controlled to not overwhelm SONG/score
        .flatMap(this::submitAndUploadToSongScore, maxInFlight, SONG_SCORE_SUBMIT_UPLOAD_PREFETCH)
        .subscribe();
  }

  public Mono<Upload> submitAndUploadToSongScore(UploadEvent uploadEvent) {
    log.debug("UploadEvent:payload for song - {}", uploadEvent.getPayload());
    return ariaClient
        .submitPayload(uploadEvent.getStudyId(), uploadEvent.getPayload())
        .flatMap(
            submitResponse ->
                withUploadContext(
                    upload -> {
                      upload.setAnalysisId(UUID.fromString(submitResponse.getAnalysisId()));
                      upload.setStatus(UploadStatus.PROCESSING);
                      return uploadService.updateUpload(upload);
                    }))
        .flatMap(
            updatedUpload ->
                ariaClient.getAnalysisFileFromSong(
                    updatedUpload.getStudyId(), updatedUpload.getAnalysisId()))
        .flatMap(
            analysisFileResponse ->
                ariaClient.initScoreUpload(
                    analysisFileResponse, uploadEvent.getSubmissionFile().getFileMd5sum()))
        .flatMap(
            scoreFileSpec ->
                ariaClient.uploadAndFinalize(
                    scoreFileSpec,
                    uploadEvent.getSubmissionFile().getContent(),
                    uploadEvent.getSubmissionFile().getFileMd5sum()))
        .flatMap(
            res ->
                withUploadContext(
                    upload ->
                        ariaClient.publishAnalysis(upload.getStudyId(), upload.getAnalysisId())))
        .flatMap(
            res ->
                withUploadContext(
                    upload -> {
                      upload.setStatus(UploadStatus.COMPLETE);
                      return uploadService.updateUpload(upload);
                    }))
        .log("SongScoreService::submitAndUploadToSongScore")
        .onErrorResume(
            throwable ->
                withUploadContext(
                    upload -> {
                      log.error(throwable.getLocalizedMessage(), throwable);
                      upload.setStatus(UploadStatus.ERROR);
                      if (throwable instanceof AriaClientException) {
                        upload.setError(throwable.toString());
                      } else if (Exceptions.isRetryExhausted(throwable)
                          && throwable.getCause() instanceof AriaClientException) {
                        upload.setError(throwable.getCause().toString());
                      } else {
                        upload.setError("Internal server error!");
                      }
                      return uploadService.updateUpload(upload);
                    }))
        .contextWrite(ctx -> ctx.put("upload", uploadEvent.getUpload()));
  }

  private <R> Mono<R> withUploadContext(Function<Upload, Mono<R>> func) {
    return Mono.deferContextual(ctx -> func.apply(ctx.get("upload")));
  }
}
