//+------------------------------------------------------------------+
//|                                              Mql5MqttPublish.mq5 |
//|                                  Copyright 2022, MetaQuotes Ltd. |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+
#property copyright "Copyright 2022, Sílvio Garbes Lara."
#property link      "https://github.com/silviogarbes/Mql5MqttPublish"
#property version   "1.00"
#property description "Use MQTT protocol with MetaTrader 5"

input int getticks=1;
input string Address = "broker.hivemq.com";
//input string Address = "127.0.0.1";
input int Port = 1883;
input string client_id = "Mql5MqttPublish";
input string topic = "Mql5MqttPublish/publishtest";

bool ExtTLS = false;

int socket;
char buffer[];
char head[];
int encodedByte;

void Publish(string content){
   char aux[];
   ArrayFree(buffer);
   ArrayResize(buffer,2,100);
   
   // Topic Name
   buffer[0] = StringLen(topic) >> 8; // String length MSB
   buffer[1] = StringLen(topic) % 256; // String length LSB
   
   // String Topic
   ArrayFree(aux);
   StringToCharArray(topic, aux, 0, StringLen(topic));
   ArrayCopy(buffer, aux, ArraySize(buffer));

   // String Content
   ArrayFree(aux);
   StringToCharArray(content, aux, 0, StringLen(content));
   ArrayCopy(buffer, aux, ArraySize(buffer));
   
   // PUBLISH Packet fixed header
   ArrayFree(head);
   ArrayResize(head,2,2);

   head[0] = char(48); // [MQTT Control Packet type (3)] [DUP] [QoS level] [Retain]
   
   // Remaining Length
   int x;
   x = ArraySize(buffer);
   do{
      encodedByte = x % 128;
      x = (int)(x / 128);
      if (x > 0){
         encodedByte = encodedByte | 128;
      }
      head[1] = char(encodedByte);
   }
   while (x > 0);
   
   // Send packages
   if(SocketSend(socket, head, ArraySize(head)) < 0){
      Print("Erro Head: ", GetLastError());
   }
   if(SocketSend(socket, buffer, ArraySize(buffer)) < 0){
      Print("Erro Buffer: ", GetLastError());
   }

}

bool ConnectMqtt(){

   socket = SocketCreate();
   if(socket!=INVALID_HANDLE){
      if(SocketConnect(socket,Address,Port,1000)){
         Print("Connected ", Address);
         
         ArrayFree(buffer);
         ArrayResize(buffer,12,100);
         
         // Protocol Name
         buffer[0] = char(0); // Length MSB (0)
         buffer[1] = char(4); // Length LSB (4)
         buffer[2] = char(77); // M
         buffer[3] = char(81); // Q
         buffer[4] = char(84); // T
         buffer[5] = char(84); // T
         
         // Protocol Level
         buffer[6] = char(4); // Level(4)
         
         // Connect Flags
         buffer[7] = char(2); // [User Name Flag] [Password Flag] [Will Retain] [Will QoS] [Will QoS] [Will Flag] [Clean Session] [Reserved]
         
         // Keep Alive
         buffer[8] = char(2); // Keep Alive MSB
         buffer[9] = char(88); // Keep Alive LSB
         
         // Client Identifier
         buffer[10] = StringLen(client_id) >> 8; // String length MSB
         buffer[11] = StringLen(client_id) % 256; // String length LSB
         char bufferClientId[];
         StringToCharArray(client_id, bufferClientId, 0, StringLen(client_id)); // String
         ArrayCopy(buffer, bufferClientId, ArraySize(buffer));
         
         // ---------- //
         
         ArrayFree(head);
         ArrayResize(head,2,2);
         
         // CONNECT Packet fixed header
         head[0] = char(16); // [MQTT Control Packet type (1)] [Reserved]
         
         // Remaining Length
         int x;
         x = ArraySize(buffer);
         do{
            encodedByte = x % 128;
            x = (int)(x / 128);
            if (x > 0){
               encodedByte = encodedByte | 128;
            }
            head[1] = char(encodedByte);
         }
         while (x > 0);
         
         // Send packages
         if(SocketSend(socket, head, ArraySize(head)) < 0){
            Print("Erro Head: ", GetLastError());
         }
         if(SocketSend(socket, buffer, ArraySize(buffer)) < 0){
            Print("Erro Buffer: ", GetLastError());
         }
         
         // CONNACK – Acknowledge connection request
         char rsp[];
         SocketRead(socket, rsp, 4, 1000); // Fixed header (2 bytes) and Variable header (2 bytes)
         
         if(rsp[0] != char(32)){ // MQTT Control Packet Type (Connect acknowledgment)
           Print("Not Connect acknowledgment");
           return false;
         }
         
         if(rsp[3] != char(0)){ // Connect Return code (Connection accepted)
           Print("Connection Refused");
           return false;
         }
         
      }else{
         return false;
      }
   }else{
      return false;
   }
   return true;
}

void DisconnectMqtt(){
   ArrayFree(head);
   ArrayResize(head,2,2);
   
   head[0] = char(224); // [MQTT Control Packet type (14)]
   head[1] = char(0); // Remaining Length (0)
   
   // Send packages
   SocketSend(socket, head, ArraySize(head));
   SocketClose(socket);
   
   Print("Disconnected");
}

int OnInit(){
   if(ConnectMqtt()){
      Publish("MQL5");
      OnTick();
      Print("INIT_SUCCEEDED");
      return(INIT_SUCCEEDED);
   }else{
      Print("INIT_FAILED ", GetLastError());
      return(INIT_FAILED);
   }   
}

void OnDeinit(const int reason){
   DisconnectMqtt();
   Print("OnDeinit ", GetLastError());
}

void OnTick(){

   int attempts = 0;
   bool success = false;
   MqlTick tick_array[];
   
   uint start;
   int received;
   int ticks;
   datetime firstticktime;
   datetime lastticktime;
   
   // Exibir os ticks
   string line;
   int tick_ini;
   int tick_fim;
   
   // Obter Ticks
   start = GetTickCount();
   received = CopyTicks(_Symbol,tick_array,COPY_TICKS_TRADE,0,getticks);
   while(attempts < 3){
      if(received != -1){
         //PrintFormat("%s: received %d ticks in %d ms",_Symbol,received,GetTickCount()-start);
         if(GetLastError()==0){
            success = true;
            break;
         }else{
            PrintFormat("%s: Ticks ainda não estão sincronizados, %d ticks recebidos por %d ms. Error=%d",_Symbol,received,GetTickCount()-start,_LastError);
         }
      }
      attempts++;
   }
   
   if(!success){
      PrintFormat("Error! Falha em receber %d ticks do %s em três tentativas. Error=%d",getticks,_Symbol,_LastError);
      return;
   }
   
   // Primeiro e ultimo tick
   ticks = ArraySize(tick_array);
   firstticktime = tick_array[0].time;
   //PrintFormat("Horário do primeiro tick = %s.%03I64u",TimeToString(firstticktime,TIME_DATE|TIME_MINUTES|TIME_SECONDS),tick_array[0].time_msc%1000);
   lastticktime = tick_array[ticks-1].time;
   //PrintFormat("Horário do último tick = %s.%03I64u",TimeToString(lastticktime,TIME_DATE|TIME_MINUTES|TIME_SECONDS),tick_array[ticks-1].time_msc%1000);
   
   tick_ini = 0;
      
   // Percorre todos os ticks
   for(int i=tick_ini; i<ticks; i++){
      line = i;
      line += " time=" + tick_array[i].time;
      line += " bid=" + tick_array[i].bid;
      line += " ask=" + tick_array[i].ask;
      line += " last=" + tick_array[i].last;
      line += " volume=" + tick_array[i].volume;
      line += " time_msc=" + tick_array[i].time_msc;
      line += " flags=" + tick_array[i].flags;
      line += " volume_real=" + tick_array[i].flags;
      
      if((tick_array[i].flags & TICK_FLAG_BUY) == TICK_FLAG_BUY){
         line += " BUY";
      }
      if((tick_array[i].flags & TICK_FLAG_SELL) == TICK_FLAG_SELL){
         line += " SELL";
      }

      printf(line);
      Publish(tick_array[i].last);
   }
   
}


