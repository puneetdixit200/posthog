import { useActions, useValues } from 'kea'

import { IconExternal } from '@posthog/icons'
import { LemonButton, LemonSwitch } from '@posthog/lemon-ui'

import { useRestrictedArea } from 'lib/components/RestrictedArea'
import { OrganizationMembershipLevel } from 'lib/constants'
import { organizationLogic } from 'scenes/organizationLogic'

import { AI_TRAINING_URL } from './aiTrainingConstants'

function AITrainingDescription({ isHipaa, isLocked }: { isHipaa: boolean; isLocked: boolean }): JSX.Element {
    if (isHipaa) {
        return <p className="mb-2">You are opted out of AI training because you are compliant with HIPAA.</p>
    }

    if (isLocked) {
        return (
            <p className="mb-2">
                Your organization's AI training preference is fixed by your contract and cannot be changed. Please
                contact us if you need to discuss this.
            </p>
        )
    }

    return (
        <div className="mb-2">
            <p>
                Enable PostHog to use anonymized aggregated data to train AI features that benefit all PostHog
                customers. Your data stays with PostHog.
            </p>
            <p className="mt-2">
                This is distinct from the <i>AI data analysis</i> consent under General, which governs whether PostHog
                AI is allowed to process your data to answer user queries at request time.
            </p>
        </div>
    )
}

export function OrganizationAITrainingOptOut(): JSX.Element {
    const { currentOrganization, currentOrganizationLoading } = useValues(organizationLogic)
    const { updateOrganization } = useActions(organizationLogic)

    const restrictionReason = useRestrictedArea({ minimumAccessLevel: OrganizationMembershipLevel.Admin })
    const isHipaa = !!currentOrganization?.is_hipaa
    const isLocked = !!currentOrganization?.is_ai_training_locked

    const disabledReason = isHipaa
        ? 'HIPAA organizations are always opted out of AI training. Please contact us if this needs to change.'
        : isLocked
          ? 'Please contact us to change this setting.'
          : restrictionReason || undefined

    const checked = !isHipaa && !!currentOrganization?.is_ai_training_opted_in

    return (
        <div className="max-w-160">
            <AITrainingDescription isHipaa={isHipaa} isLocked={isLocked} />
            <div className="my-4">
                <LemonSwitch
                    label="Enable AI training on anonymized data"
                    data-attr="organization-ai-training-opt-in"
                    onChange={(value) => {
                        updateOrganization({ is_ai_training_opted_in: value })
                    }}
                    checked={checked}
                    disabledReason={disabledReason}
                    loading={currentOrganizationLoading}
                    bordered
                />
            </div>
            <LemonButton type="primary" className="inline-block" sideIcon={<IconExternal />} to={AI_TRAINING_URL}>
                What's this?
            </LemonButton>
        </div>
    )
}
