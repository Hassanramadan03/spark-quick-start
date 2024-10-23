import { Injectable } from "@nestjs/common";
import { ITransaction } from "./dtos/transaction.dto";

@Injectable()
export class TransactionsService {
  onCreateTransaction(transaction: ITransaction) {
    console.log(transaction);
  }
  onDeleteTransaction(transcationId: string) {
    console.log(transcationId);
  }
}
