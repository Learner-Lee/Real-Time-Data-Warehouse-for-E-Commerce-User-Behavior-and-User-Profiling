package r1;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 用 Flink 1.20.5 的 SQL 规划器校验 sql/ 目录下的作业：替换占位符 → 执行 SET 和建表 → 对 INSERT 做 EXPLAIN。
 * EXPLAIN 会走完解析、类型检查、优化和连接器参数校验，但不会真的连接 Kafka、HBase、MySQL，
 * 所以在没有环境的 Mac 上也能提前发现 SQL 写错的地方。
 *
 * <p>唯一的例外是 Doris：它的连接器生成执行计划时就要连上 FE，所以这里把 Doris 表换成 blackhole
 * （表结构不变，INSERT 的字段和类型照样校验；Doris 自己的参数由虚拟机上的冒烟测试验证）。
 */
class SqlPlanTest {

    private static final Path SQL_DIR = Path.of(System.getProperty("user.dir")).resolve("../sql").normalize();
    private static final Pattern SET = Pattern.compile("(?is)SET\\s+'([^']+)'\\s*=\\s*'([^']*)'");
    private static final Pattern STATEMENT_SET_HEAD = Pattern.compile("(?is)^EXECUTE\\s+STATEMENT\\s+SET\\s+BEGIN\\s+");
    private static final Pattern DORIS_OPTIONS = Pattern.compile("(?s)WITH\\s*\\(\\s*'connector'\\s*=\\s*'doris'.*?\\);");

    @TempDir
    Path dataDir;

    @ParameterizedTest
    @ValueSource(strings = {"00_load_dims.sql", "10_dwd_user_behavior.sql", "20_dws_traffic.sql", "30_dws_trade.sql"})
    void sqlFilePlansSuccessfully(String file) throws IOException {
        // 维表作业要读文件：放两份最小的样例，模拟 prepare_data.py 的输出
        Path r1 = Files.createDirectories(dataDir.resolve("r1"));
        Files.writeString(r1.resolve("dim_user.csv"), "100,gold,深圳\n");
        Files.writeString(r1.resolve("dim_item.csv"), "200,10,59.90\n");

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        StatementSet statementSet = null;
        int inserts = 0;
        for (String statement : splitStatements(render(Files.readString(SQL_DIR.resolve(file))))) {
            Matcher set = SET.matcher(statement);
            if (set.matches()) {
                tEnv.getConfig().set(set.group(1), set.group(2));
                continue;
            }
            Matcher head = STATEMENT_SET_HEAD.matcher(statement);
            if (head.find()) {
                statementSet = tEnv.createStatementSet();
                statement = statement.substring(head.end());
            }
            if (statement.equalsIgnoreCase("END")) {
                assertFalse(statementSet.explain().isBlank());
                statementSet = null;
            } else if (statement.toUpperCase().startsWith("INSERT")) {
                inserts++;
                if (statementSet != null) {
                    statementSet.addInsertSql(statement);
                } else {
                    assertFalse(tEnv.explainSql(statement).isBlank());
                }
            } else {
                tEnv.executeSql(statement);   // CREATE TABLE 等
            }
        }
        assertTrue(inserts > 0, file + " 里没有 INSERT");
        assertNull(statementSet, file + " 的 EXECUTE STATEMENT SET 缺少 END");
    }

    private String render(String sql) {
        return DORIS_OPTIONS.matcher(sql).replaceAll("WITH ('connector' = 'blackhole');")
                .replace("@VM_HOST@", "localhost")
                .replace("@MYSQL_APP_USER@", "r1")
                .replace("@MYSQL_APP_PASSWORD@", "test")
                .replace("@LABEL_SUFFIX@", "test")
                .replace("@DATA_DIR@", dataDir.toString());
    }

    /** 去掉整行注释，按行尾的分号切分语句 */
    private static List<String> splitStatements(String sql) {
        StringBuilder body = new StringBuilder();
        for (String line : sql.split("\n")) {
            if (!line.strip().startsWith("--")) {
                body.append(line).append('\n');
            }
        }
        List<String> statements = new ArrayList<>();
        for (String statement : body.toString().split(";\\s*(\n|$)")) {
            if (!statement.isBlank()) {
                statements.add(statement.strip());
            }
        }
        return statements;
    }

    /** 防止 sql/ 目录里有作业文件忘了加进上面的列表 */
    @org.junit.jupiter.api.Test
    void everyFlinkSqlFileIsCovered() throws IOException {
        List<String> covered = Arrays.asList("00_load_dims.sql", "10_dwd_user_behavior.sql", "20_dws_traffic.sql", "30_dws_trade.sql");
        try (var files = Files.list(SQL_DIR)) {
            files.map(p -> p.getFileName().toString())
                    .filter(name -> name.matches("\\d\\d_.*\\.sql"))
                    .forEach(name -> assertTrue(covered.contains(name), name + " 没有加进 SqlPlanTest"));
        }
    }
}
