/*
 * Copyright (c) 2021 The Ontario Institute for Cancer Research. All rights reserved
 *
 * This program and the accompanying materials are made available under the terms of the GNU Affero General Public License v3.0.
 * You should have received a copy of the GNU Affero General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.cancogenvirusseq.muse.service;

import java.time.OffsetDateTime;
import java.util.Set;
import java.util.UUID;
import lombok.val;
import org.cancogenvirusseq.muse.repository.model.Upload;
import org.cancogenvirusseq.muse.repository.model.UploadStatus;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class UploadServiceTest {

  final UUID userOne = UUID.randomUUID();
  final UUID submissionOne = UUID.randomUUID();
  final UUID userTwo = UUID.randomUUID();
  final UUID submissionTwo = UUID.randomUUID();

  @Test
  void filterUserAndSubmissionIdPass() {
    val uploadEvent = makeTestUploadFor(userOne, submissionOne);

    val uploadEvents = Flux.just(uploadEvent);

    StepVerifier.create(
            UploadService.filterForUserAndMaybeSubmissionId(submissionOne, userOne.toString())
                .apply(uploadEvents))
        .expectNext(uploadEvent)
        .verifyComplete();
  }

  @Test
  void filterUserAndSubmissionIdOnUserAndSubmission() {
    val uploadEventOne = makeTestUploadFor(userOne, submissionOne);
    val uploadEventTwo = makeTestUploadFor(userTwo, submissionTwo);

    val uploadEvents = Flux.just(uploadEventOne, uploadEventTwo);

    StepVerifier.create(
            UploadService.filterForUserAndMaybeSubmissionId(submissionTwo, userTwo.toString())
                .apply(uploadEvents))
        .expectNext(uploadEventTwo)
        .verifyComplete();
  }

  @Test
  void filterUserAndSubmissionIdOnUserOnly() {
    val uploadEventOne = makeTestUploadFor(userOne, submissionOne);
    val uploadEventTwo = makeTestUploadFor(userTwo, submissionTwo);

    val uploadEvents = Flux.just(uploadEventOne, uploadEventTwo);

    StepVerifier.create(
            UploadService.filterForUserAndMaybeSubmissionId(null, userTwo.toString())
                .apply(uploadEvents))
        .expectNext(uploadEventTwo)
        .verifyComplete();
  }

  private Upload makeTestUploadFor(UUID userId, UUID submissionId) {
    return Upload.builder()
        .uploadId(UUID.randomUUID())
        .createdAt(OffsetDateTime.now())
        .originalFilePair(Set.of())
        .status(UploadStatus.QUEUED)
        .studyId("MUSE-TEST")
        .submissionId(submissionId)
        .submitterSampleId("MuseId")
        .userId(userId)
        .build();
  }
}
