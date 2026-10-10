package ibhist;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ContractDetailsAction extends ActionBase {
    private static final Logger log = LogManager.getLogger(ContractDetailsAction.class.getSimpleName());

    private Contract contract;
    private ContractDetails contractDetails;

    @AssistedInject
    public ContractDetailsAction(@Assisted ActionContext context, @Assisted Contract contract) {
        super(context.client(), context.idGenerator(), context.queue());
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
        log.info("reqContractDetails reqId = {} symbol = {}", requestId, contract.symbol());
        client.reqContractDetails(requestId, contract);
    }

    public void onContractDetails(ContractDetails contractDetails) {
        this.contractDetails = contractDetails;
        this.contract = contractDetails.contract();
        // tz = US/Central
        log.info("reqContractDetails reqId = {} symbol = {} tz = {}", requestId, contract.symbol(), contractDetails.timeZoneId());
    }
}
