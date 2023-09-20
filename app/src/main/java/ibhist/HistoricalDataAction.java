package ibhist;

import com.ib.client.Bar;
import com.ib.client.Contract;
import com.ib.client.EClientSocket;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class HistoricalDataAction extends ActionBase {
    public static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyyMMdd-HH:mm:ss");
    private final Consumer<HistoricalDataAction> continuation;

    private final Contract contract;

    private final List<Bar> bars = new ArrayList<>();

    public HistoricalDataAction(EClientSocket client, AtomicInteger idGenerator, Contract contract, Consumer<HistoricalDataAction> continuation) {
        super(client, idGenerator);
        this.continuation = continuation;
        this.contract = contract;
    }

    public Contract getContract() {
        return contract;
    }

    public List<Bar> getBars() {
        return bars;
    }

    @Override
    public void request() {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.add(Calendar.MONTH, -1);
        String endDateTime = FORMAT.format(cal.getTime());
        //TODO test the format for end date
//        endDateTime = "20230919 22:00:00";
        client.reqHistoricalData(requestId, contract, endDateTime, "10 D", "1 min", "TRADES", 0, 1, false, null);
    }

    @Override
    public void process() {
        continuation.accept(this);
    }

    @Override
    public void cancel() {
        client.cancelHistoricalData(requestId);
    }

    public void process(Bar bar) {
        bars.add(bar);
    }
}
