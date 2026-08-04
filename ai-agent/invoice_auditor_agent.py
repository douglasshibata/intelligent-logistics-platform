import os
import json
import requests
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

# =====================================================================
# DATA CONTRACTS & SCHEMAS
# =====================================================================

class CargoManifest(BaseModel):
    """Representa o manifesto físico esperado da carga planejado no ERP."""
    manifest_id: str = Field(..., description="ID único do manifesto de carga.")
    expected_weight_kg: float = Field(..., description="Peso total esperado em quilogramas.")
    temperature_controlled: bool = Field(..., description="Informa se a carga necessita de refrigeração.")
    origin_city: str = Field(..., description="Cidade de origem.")
    destination_city: str = Field(..., description="Cidade de destino.")
    tax_category_rate: float = Field(..., description="Alíquota fiscal esperada para a mercadoria.")
    total_declared_value: float = Field(..., description="Valor total declarado das mercadorias.")


class BillOfLadingInvoice(BaseModel):
    """Representa a nota fiscal de transporte (CT-e / Bill of Lading) emitida pela transportadora."""
    invoice_number: str = Field(..., description="Número da nota fiscal / conhecimento de transporte.")
    carrier_name: str = Field(..., description="Razão social da transportadora.")
    declared_weight_kg: float = Field(..., description="Peso declarado no documento fiscal.")
    refrigerated_service_flag: bool = Field(..., description="Informa se o serviço contratado inclui refrigeração.")
    origin_address: str = Field(..., description="Endereço de partida.")
    destination_address: str = Field(..., description="Endereço de entrega.")
    applied_tax_rate: float = Field(..., description="Alíquota tributária aplicada na emissão.")
    invoice_total_value: float = Field(..., description="Valor total cobrado na fatura de transporte.")


class AuditReport(BaseModel):
    """Relatório estruturado de auditoria emitido pelo agente autônomo de IA."""
    is_compliant: bool = Field(..., description="Status de conformidade do documento analisado.")
    severity: str = Field(..., description="Gravidade da infração ('LOW', 'MEDIUM', 'HIGH', 'NONE').")
    identified_discrepancies: List[str] = Field(default=[], description="Lista com as discrepâncias encontradas.")
    financial_impact_estimate: float = Field(default=0.0, description="Estimativa de impacto financeiro / multa em reais.")
    agent_reasoning: str = Field(..., description="Justificativa lógica detalhada gerada pelo Agente de IA.")


# =====================================================================
# AGENT IMPLEMENTATION (AGENT STARTER KIT PATTERN)
# =====================================================================

class LogisticsInvoiceAuditorAgent:
    """
    Agente de IA Autônomo para Auditoria de Documentos de Transporte (CT-e / Bill of Lading).
    Utiliza vLLM ou Ollama local para comparar dados físicos versus faturamento tributário.
    """
    def __init__(self, ollama_url: str = "http://localhost:11434", model_name: str = "qwen2.5-coder:7b"):
        self.ollama_url = ollama_url
        self.model_name = model_name

    def audit_invoice(self, manifest: CargoManifest, invoice: BillOfLadingInvoice) -> AuditReport:
        """
        Executa a auditoria comparativa entre o manifesto esperado e o documento fiscal de transporte.
        """
        system_prompt = (
            "Você é o Agente de Auditoria Fiscal e Operacional da Plataforma de Inteligência Logística.\n"
            "Seu papel é analisar o manifesto planejado de carga (físico/financeiro esperado) e compará-lo "
            "com a Nota de Conhecimento de Transporte (CT-e / Bill of Lading) emitida pela transportadora.\n"
            "Você deve identificar qualquer inconsistência operacional, discrepâncias de peso, incompatibilidades "
            "de cadeia de refrigeração (cadeia de frio), desvios geográficos grosseiros ou alíquotas de impostos incorretas.\n"
            "Retorne a resposta estritamente no formato JSON estruturado correspondente ao AuditReport:\n"
            "{\n"
            "  \"is_compliant\": bool,\n"
            "  \"severity\": \"LOW\" | \"MEDIUM\" | \"HIGH\" | \"NONE\",\n"
            "  \"identified_discrepancies\": [\"lista\", \"de\", \"discrepancias\"],\n"
            "  \"financial_impact_estimate\": float,\n"
            "  \"agent_reasoning\": \"sua justificativa detalhada em português\"\n"
            "}\n"
            "Importante: Responda apenas com o objeto JSON puro, sem markdown wraps ```json ou qualquer texto explicativo extra."
        )

        user_content = f"""
=== MANIFESTO ESPERADO (FÍSICO/PLANEJADO) ===
ID do Manifesto: {manifest.manifest_id}
Peso Esperado: {manifest.expected_weight_kg} kg
Cadeia de Frio Requerida: {manifest.temperature_controlled}
Cidade Origem: {manifest.origin_city}
Cidade Destino: {manifest.destination_city}
Alíquota Esperada: {manifest.tax_category_rate}%
Valor Total Carga: R$ {manifest.total_declared_value}

=== CONHECIMENTO DE TRANSPORTE EMITIDO (CT-e / BILL OF LADING) ===
Número da Fatura: {invoice.invoice_number}
Transportadora: {invoice.carrier_name}
Peso Declarado na Nota: {invoice.declared_weight_kg} kg
Serviço Refrigerado Contratado: {invoice.refrigerated_service_flag}
Endereço Origem: {invoice.origin_address}
Endereço Destino: {invoice.destination_address}
Alíquota de Imposto Aplicada: {invoice.applied_tax_rate}%
Valor da Fatura de Transporte: R$ {invoice.invoice_total_value}
"""

        payload = {
            "model": self.model_name,
            "prompt": f"{system_prompt}\n\n{user_content}",
            "stream": False,
            "options": {
                "temperature": 0.1  # Baixa temperatura para manter a consistência e conformidade estrutural
            }
        }

        try:
            # Faz a chamada para a API local do Ollama
            response = requests.post(f"{self.ollama_url}/api/generate", json=payload, timeout=30)
            if response.status_code == 200:
                raw_text = response.json().get("response", "").strip()
                # Tratamento de segurança caso o modelo retorne bloco markdown de código
                if raw_text.startswith("```"):
                    raw_text = raw_text.replace("```json", "").replace("```", "").strip()

                parsed_json = json.loads(raw_text)
                return AuditReport(**parsed_json)
            else:
                return self._generate_fallback_report("Erro de comunicação com o servidor de IA local (Status: {}).".format(response.status_code))
        except Exception as e:
            # Fallback determinístico de auditoria caso o serviço de LLM esteja inacessível/offline
            return self._execute_deterministic_rule_audit(manifest, invoice, str(e))

    def _execute_deterministic_rule_audit(self, manifest: CargoManifest, invoice: BillOfLadingInvoice, error_msg: str) -> AuditReport:
        """
        Fallback determinístico baseado em regras de código estruturadas para garantir resiliência
        mesmo se o serviço de LLM / Ollama estiver totalmente indisponível.
        """
        discrepancies = []
        severity = "NONE"
        impact = 0.0

        # Regra 1: Validação de cadeia de frio (Cold Chain)
        if manifest.temperature_controlled and not invoice.refrigerated_service_flag:
            discrepancies.append("Cadeia de frio requerida pelo manifesto, mas transportadora emitiu nota como carga seca!")
            severity = "HIGH"
            impact += manifest.total_declared_value * 0.5 # estimativa de perda de 50% por deterioração

        # Regra 2: Peso divergente acima de 5%
        weight_diff_pct = abs(manifest.expected_weight_kg - invoice.declared_weight_kg) / manifest.expected_weight_kg
        if weight_diff_pct > 0.05:
            discrepancies.append(f"Discrepância grave de peso! Planejado: {manifest.expected_weight_kg}kg, Faturado: {invoice.declared_weight_kg}kg (Diferença de {weight_diff_pct*100:.1f}%)")
            severity = "HIGH" if severity == "NONE" else "HIGH"
            impact += 1500.00 # Multa regulatória padrão por excesso/divergência de peso

        # Regra 3: Diferença tributária
        if manifest.tax_category_rate != invoice.applied_tax_rate:
            discrepancies.append(f"Alíquota fiscal incoerente. Esperada: {manifest.tax_category_rate}%, Aplicada: {invoice.applied_tax_rate}%")
            if severity == "NONE":
                severity = "MEDIUM"
            impact += abs(manifest.total_declared_value * (manifest.tax_category_rate - invoice.applied_tax_rate) / 100.0)

        # Regra 4: Desvios de Cidades
        if manifest.destination_city.lower() not in invoice.destination_address.lower():
            discrepancies.append(f"Cidade de destino diverge! Planejado: {manifest.destination_city}, Endereço nota: {invoice.destination_address}")
            severity = "HIGH"

        is_compliant = len(discrepancies) == 0
        reasoning = (
            f"Auditoria Determinística de Segurança executada devido ao indisponibilidade do modelo LLM local ({error_msg}). "
            f"Análise baseada nas regras de governança logística da corporação."
        )

        return AuditReport(
            is_compliant=is_compliant,
            severity=severity,
            identified_discrepancies=discrepancies,
            financial_impact_estimate=impact,
            agent_reasoning=reasoning
        )

    def _generate_fallback_report(self, reason: str) -> AuditReport:
        return AuditReport(
            is_compliant=False,
            severity="MEDIUM",
            identified_discrepancies=["Erro técnico na chamada do LLM."],
            financial_impact_estimate=0.0,
            agent_reasoning=f"Falha ao acionar motor de inferência local: {reason}"
        )


# =====================================================================
# VERIFICATION SIMULATION
# =====================================================================

if __name__ == "__main__":
    print("=== INICIANDO SIMULAÇÃO DE AUDITORIA DE DOCUMENTOS FISCAIS DE CARGA ===")

    # Exemplo 1: Caso em conformidade completa
    manifest_ok = CargoManifest(
        manifest_id="MNF-991A",
        expected_weight_kg=12500.0,
        temperature_controlled=True,
        origin_city="Campinas",
        destination_city="Rio de Janeiro",
        tax_category_rate=12.0,
        total_declared_value=250000.0
    )

    invoice_ok = BillOfLadingInvoice(
        invoice_number="CTE-100293",
        carrier_name="Logistica Brasil S.A.",
        declared_weight_kg=12500.0,
        refrigerated_service_flag=True,
        origin_address="Galpão Campinas - Rod. Anhanguera, KM 98",
        destination_address="CD Rio de Janeiro - Av. Brasil, 45000",
        applied_tax_rate=12.0,
        invoice_total_value=4200.0
    )

    # Exemplo 2: Caso de grave quebra de cadeia de frio e divergência tributária
    manifest_anomaly = CargoManifest(
        manifest_id="MNF-442B",
        expected_weight_kg=8000.0,
        temperature_controlled=True, # Exige cadeia de frio para medicamentos termolábeis
        origin_city="São Paulo",
        destination_city="Curitiba",
        tax_category_rate=18.0,
        total_declared_value=450000.0
    )

    invoice_anomaly = BillOfLadingInvoice(
        invoice_number="CTE-294012",
        carrier_name="Transportes Rapido Seco Ltda",
        declared_weight_kg=9200.0, # Peso divergente em 15%
        refrigerated_service_flag=False, # Falha grave: transportando medicamentos críticos em caminhão comum
        origin_address="Doca Central São Paulo - Av. Chucri Zaidan, 100",
        destination_address="CD Curitiba - Rodovia do Cafe, KM 12",
        applied_tax_rate=12.0, # Alíquota menor (possível evasão ou erro de classificação fiscal)
        invoice_total_value=3100.0
    )

    auditor = LogisticsInvoiceAuditorAgent()

    print("\n[TESTE 1] Executando auditoria em nota em conformidade...")
    report_ok = auditor.audit_invoice(manifest_ok, invoice_ok)
    print(f"Status Complacência: {report_ok.is_compliant}")
    print(f"Gravidade: {report_ok.severity}")
    print(f"Discrepâncias: {report_ok.identified_discrepancies}")
    print(f"Impacto Estimado: R$ {report_ok.financial_impact_estimate:.2f}")
    print(f"Raciocínio do Agente:\n{report_ok.agent_reasoning}\n")

    print("[TESTE 2] Executando auditoria em nota com anomalias graves detectadas...")
    report_anomaly = auditor.audit_invoice(manifest_anomaly, invoice_anomaly)
    print(f"Status Complacência: {report_anomaly.is_compliant}")
    print(f"Gravidade: {report_anomaly.severity}")
    print(f"Discrepâncias: {report_anomaly.identified_discrepancies}")
    print(f"Impacto Estimado: R$ {report_anomaly.financial_impact_estimate:.2f}")
    print(f"Raciocínio do Agente:\n{report_anomaly.agent_reasoning}\n")
    print("=== SIMULAÇÃO CONCLUÍDA COM SUCESSO ===")
