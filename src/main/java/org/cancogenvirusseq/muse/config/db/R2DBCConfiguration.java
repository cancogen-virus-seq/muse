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

package org.cancogenvirusseq.muse.config.db;

import com.google.common.base.Strings;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.codec.EnumCodec;
import io.r2dbc.spi.ConnectionFactory;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import lombok.val;
import org.cancogenvirusseq.muse.repository.model.UploadStatus;
import org.cancogenvirusseq.muse.repository.model.UploadStatusConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.connectionfactory.init.ConnectionFactoryInitializer;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;

@Configuration
@EnableR2dbcRepositories(basePackages = "org.cancogenvirusseq.muse.repository")
public class R2DBCConfiguration extends AbstractR2dbcConfiguration {

  @Autowired private PostgresProperties postgresProperties;

  @Bean
  ConnectionFactoryInitializer initializer(ConnectionFactory connectionFactory) {
    ConnectionFactoryInitializer initializer = new ConnectionFactoryInitializer();
    initializer.setConnectionFactory(connectionFactory);
    return initializer;
  }

  @Bean
  public PostgresqlConnectionFactory psqlConnectionFactory() {
    val postgresqlConnectionConfiguration = PostgresqlConnectionConfiguration.builder();

    postgresqlConnectionConfiguration
        .host(postgresProperties.getHost())
        .port(postgresProperties.getPort())
        .database(postgresProperties.getDatabase());

    if (!Strings.isNullOrEmpty(postgresProperties.getUsername())) {
      postgresqlConnectionConfiguration.username(postgresProperties.getUsername());
    }

    if (!Strings.isNullOrEmpty(postgresProperties.getPassword())) {
      postgresqlConnectionConfiguration.password(postgresProperties.getPassword());
    }

    // register sql enum upload_status to Java enum UploadStatus
    val codecRegistrar = EnumCodec.builder().withEnum("upload_status", UploadStatus.class).build();

    return new PostgresqlConnectionFactory(
        postgresqlConnectionConfiguration.codecRegistrar(codecRegistrar).build());
  }

  @Override
  @Bean
  @Primary
  public ConnectionFactory connectionFactory() {
    val configuration =
        ConnectionPoolConfiguration.builder(psqlConnectionFactory())
            .maxIdleTime(Duration.ofMillis(1000))
            .maxSize(20)
            .build();

    return new ConnectionPool(configuration);
  }

  @Override
  protected List<Object> getCustomConverters() {
    // set custom converter UploadStatus for enum resolution
    return Collections.singletonList(new UploadStatusConverter());
  }
}
