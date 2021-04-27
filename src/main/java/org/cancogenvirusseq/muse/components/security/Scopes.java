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

package org.cancogenvirusseq.muse.components.security;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.NonNull;
import org.cancogenvirusseq.muse.config.websecurity.AuthProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

@Component
public class Scopes {

  private final AuthProperties.ScopesConfig scopesConfig;
  public Predicate<String> isValidScope;

  public Scopes(@NonNull AuthProperties authProperties) {
    this.scopesConfig = authProperties.getScopes();

    final Predicate<String> startsWithStudyPrefix =
        (String scope) -> scope.startsWith(scopesConfig.getStudy().getPrefix());

    final Predicate<String> endsWithStudySuffix =
        (String scope) -> scope.endsWith(scopesConfig.getStudy().getSuffix());

    final Predicate<String> isStudyScope = startsWithStudyPrefix.and(endsWithStudySuffix);

    final Predicate<String> isSystemScope =
        (String scope) -> scope.equals(scopesConfig.getSystem());

    this.isValidScope = isSystemScope.or(isStudyScope);
  }

  @Bean
  public Function<Authentication, Boolean> readWriteScopeChecker() {
    return authentication ->
        authentication.getAuthorities().stream().map(Objects::toString).anyMatch(isValidScope);
  }
}
