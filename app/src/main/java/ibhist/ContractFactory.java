package ibhist;

import com.ib.client.Contract;

public interface ContractFactory {
    Contract newFutureContract(String symbol, String contractMonth);
    Contract newIndex(String symbol);
}
