package ibhist;

import com.ib.client.Contract;

public class ContractFactoryImpl implements ContractFactory {
    @Override
    public Contract newFutureContract(String symbol, String contractMonth) {
        //! [futcontract]
        Contract contract = new Contract();
        contract.symbol(symbol);
        contract.lastTradeDateOrContractMonth(contractMonth);
        contract.secType("FUT");
        contract.currency("USD");
        contract.exchange("CME");
        //! [futcontract]
        return contract;
    }

    @Override
    public Contract newIndex(String symbol) {
        Contract contract = new Contract();
        contract.symbol(symbol);
        contract.secType("IND");
        contract.currency("USD");
        contract.exchange("NYSE");
        return contract;
    }
}
