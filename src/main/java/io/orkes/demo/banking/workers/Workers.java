package io.orkes.demo.banking.workers;

import com.netflix.conductor.common.run.*;
import com.netflix.conductor.sdk.workflow.task.*;
import com.zaxxer.hikari.*;

import javax.sql.*;
import java.math.*;
import java.sql.*;

public class Workers {

    static String url = "CHANGE_ME";
    static String user = "CHANGE_ME";
    static String password = "CHANGE_ME";

    private final DataSource dataSource;

    public Workers() {
        HikariConfig config = new HikariConfig();
        config.setAutoCommit(true);
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        config.setDriverClassName("org.postgresql.Driver");
        config.setMaximumPoolSize(32);
        this.dataSource = new HikariDataSource(config);
    }

    @WorkerTask("save_exchange_rate")
    public @OutputParam("return_code") String saveExchangeRates(@InputParam("i_ccy") String src,
                                    @InputParam("o_ccy") String target, @InputParam("ex_rate") BigDecimal rate)  {
        try {
            String INSERT = "insert into exchange_rate as er (src_currency, target_src_currency, rate) values (?,?,?) " +
                    "on conflict(src_currency, target_src_currency) do update set rate = ? where er.src_currency = ? and er.target_src_currency = ?";
            try (Connection connection = dataSource.getConnection()) {
                PreparedStatement pstmt = connection.prepareStatement(INSERT);


                pstmt.setString(1, src);
                pstmt.setString(2, target);
                pstmt.setBigDecimal(3, rate);

                //on conflict
                pstmt.setBigDecimal(4, rate);
                pstmt.setString(5, src);
                pstmt.setString(6, target);


                pstmt.executeUpdate();

            }
            return "SUCCESS";
        } catch (Exception e) {
            e.printStackTrace();
            return "FAILURE";
        }
    }

    @WorkerTask("workflow_completion_listener")
    public void workflowCompleted(@InputParam("workflow") Workflow workflow) {
        System.out.println("Workflow completed " + workflow);
    }
}
