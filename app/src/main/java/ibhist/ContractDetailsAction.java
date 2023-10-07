package ibhist;

import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import com.ib.client.EClientSocket;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class ContractDetailsAction extends ActionBase {

    private Contract contract;

    private ContractDetails contractDetails;
    private final Consumer<ContractDetailsAction> continuation;
    public ContractDetailsAction(EClientSocket client, AtomicInteger idGenerator, BlockingQueue<Action> queue, Contract contract, Consumer<ContractDetailsAction> continuation) {
        super(client, idGenerator, queue);
        this.contract = contract;
        this.continuation = continuation;
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

//    @Override
//    public void process() {
//        continuation.accept(this);
//    }

    public void process(ContractDetails contractDetails) {
        this.contractDetails = contractDetails;
        this.contract = contractDetails.contract();
    }
}
