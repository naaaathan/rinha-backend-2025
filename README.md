# Rinha de Backend 2025

Este repositório contém a minha implementação para a **Rinha de Backend 2025**, um torneio entre desenvolvedores focado em criar o código backend mais performático e eficiente.

A competição desafia cada participante a entregar a melhor solução possível, buscando otimização, escalabilidade e qualidade do código conforme o desafio proposto.

## Sobre a implementação

Minha solução foi desenvolvida em **Java** utilizando o framework **Quarkus** e tem como foco a alta performance e resiliência sob carga extrema. Os principais destaques técnicos são:

- **Banco de dados Redis**: Utilizo Redis para armazenamento eficiente dos pagamentos, explorando sorted sets e scripts Lua para consultas agregadas rápidas.
- **Virtual Threads (Java)**: O processamento das requisições é feito de forma concorrente usando virtual threads, permitindo alta escalabilidade e baixo consumo de recursos.
- **Persistência assíncrona em lote**: As operações de escrita no Redis são agrupadas em lotes pelo componente `RedisAsyncWriter`, reduzindo overhead e aumentando o throughput.
- **Resiliência e tolerância a falhas**: Uso de Circuit Breaker e Fallback (Microprofile Fault Tolerance) para garantir disponibilidade mesmo em cenários de falha de serviços externos.
- **Scripts Lua para agregação**: Consultas de resumo são realizadas diretamente no Redis com scripts Lua, acelerando operações de agregação e estatísticas.

Sinta-se à vontade para explorar o código, revisar as estratégias utilizadas e sugerir melhorias!

---

Boa sorte a todos os participantes!