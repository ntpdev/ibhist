package ibhist;

import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import com.ib.client.EClientSocket;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class ContractDetailsAction extends ActionBase {

    private Contract contract;
    private ContractDetails contractDetails;

    public ContractDetailsAction(EClientSocket client, AtomicInteger idGenerator, BlockingQueue<Action> queue, Contract contract) {
        super(client, idGenerator, queue);
        this.contract = contract;
    }

    public Contract getContract() {
        return contract;
    }

    public ContractDetails getContractDetails() {
        return contractDetails;
    }

    @Override
    public void makeRequest() {
        client.reqContractDetails(requestId, contract);
    }

    public void process(ContractDetails contractDetails) {
        this.contractDetails = contractDetails;
        this.contract = contractDetails.contract();
    }
}
